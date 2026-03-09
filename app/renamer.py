"""LLM-powered file renamer using Ollama — extracts metadata for semantic dedup."""

import os
import re
import json
import logging
import httpx
from app.config import settings
from app.database import Database

logger = logging.getLogger(__name__)

# Quality scoring: higher = better
RESOLUTION_SCORES = {
    "2160p": 100, "4k": 100, "uhd": 100,
    "1080p": 80, "fhd": 80,
    "720p": 60, "hd": 60,
    "480p": 40, "sd": 40,
    "360p": 20,
}

SOURCE_SCORES = {
    "bluray": 50, "blu-ray": 50, "remux": 55,
    "web-dl": 40, "webdl": 40, "webrip": 35,
    "hdtv": 30,
    "dvdrip": 20, "dvd": 20,
    "cam": 5, "ts": 5, "telesync": 5,
}

CODEC_SCORES = {
    "av1": 15, "x265": 12, "hevc": 12, "h265": 12,
    "x264": 10, "h264": 10, "avc": 10,
    "xvid": 5, "divx": 5,
}

SYSTEM_PROMPT = """You are a media file naming expert. Your job is to analyze media filenames and do two things:
1. Suggest a clean, descriptive new filename
2. Extract structured metadata about the content

Guidelines for naming:
- Determine the media type (movie, TV show, music, documentary, etc.) from context clues
- Extract meaningful information: title, year, season/episode, resolution, codec, source, etc.
- Choose a clear, consistent naming style appropriate to the media type
- Remove junk characters, release group tags, and unnecessary information
- Keep the file extension unchanged
- If the original name is already clean and descriptive, keep it as-is
- If you cannot confidently determine what the file is, return the original name unchanged

Guidelines for metadata extraction:
- content_title: The core identity of the content (e.g. "Inception", "Breaking Bad S01E01", "Artist - Song Title"). This should be the SAME for different copies of the same content at different qualities.
- media_type: one of "movie", "tv", "music", "other"
- resolution: e.g. "2160p", "1080p", "720p", "480p", "SD", or null
- codec: e.g. "x265", "x264", "AV1", or null
- source: e.g. "BluRay", "WEB-DL", "HDTV", "DVDRip", or null

CRITICAL: The content_title must be NORMALIZED so that two copies of the same content will have EXACTLY the same content_title regardless of quality or source. For example:
- "Inception.2010.1080p.BluRay.x264.mkv" → content_title: "Inception (2010)"
- "Inception.2010.720p.WEB-DL.x265.mkv" → content_title: "Inception (2010)"
- "Breaking.Bad.S01E01.720p.mkv" → content_title: "Breaking Bad S01E01"
- "breaking bad - s01e01 - pilot.1080p.mkv" → content_title: "Breaking Bad S01E01"

You must respond with valid JSON only. No explanation, no markdown, just a JSON array."""

USER_PROMPT_TEMPLATE = """Analyze these filenames. For each, suggest a better name AND extract metadata. Return a JSON array.

Filenames:
{filenames}

Respond with ONLY a JSON array like:
[
  {{
    "original": "old.name.mkv",
    "suggested": "New Name.mkv",
    "content_title": "Normalized Title (Year)",
    "media_type": "movie",
    "resolution": "1080p",
    "codec": "x264",
    "source": "BluRay"
  }},
  ...
]"""


class Renamer:
    def __init__(self, db: Database):
        self.db = db
        self.ollama_url = settings.ollama_url.rstrip("/")
        self.model = settings.ollama_model
        self.batch_size = settings.rename_batch_size
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def stop(self):
        self._running = False

    def _compute_quality_score(self, resolution: str | None, source: str | None, codec: str | None) -> int:
        """Compute a numeric quality score from resolution, source, and codec."""
        score = 0
        if resolution:
            score += RESOLUTION_SCORES.get(resolution.lower(), 0)
        if source:
            score += SOURCE_SCORES.get(source.lower(), 0)
        if codec:
            score += CODEC_SCORES.get(codec.lower(), 0)
        return score

    async def _call_ollama(self, filenames: list[str]) -> list[dict]:
        """Send a batch of filenames to Ollama and parse the response."""
        filename_list = "\n".join(f"- {name}" for name in filenames)
        user_prompt = USER_PROMPT_TEMPLATE.format(filenames=filename_list)

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 4096,
            },
            "format": "json",
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self.ollama_url}/api/chat",
                json=payload,
            )
            response.raise_for_status()

        result = response.json()
        content = result.get("message", {}).get("content", "")

        # Parse the JSON response
        try:
            parsed = json.loads(content)
            # Handle case where model wraps array in an object
            if isinstance(parsed, dict):
                for v in parsed.values():
                    if isinstance(v, list):
                        parsed = v
                        break
            if not isinstance(parsed, list):
                logger.warning(f"Unexpected response format: {content[:200]}")
                return []
            return parsed
        except json.JSONDecodeError:
            match = re.search(r'\[.*\]', content, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
            logger.warning(f"Failed to parse LLM response: {content[:200]}")
            return []

    def _sanitize_filename(self, name: str) -> str:
        """Remove characters that are illegal in common filesystems."""
        illegal = r'[<>:"/\\|?*\x00-\x1f]'
        name = re.sub(illegal, '', name)
        name = re.sub(r'\.{2,}', '.', name)
        name = re.sub(r'\s{2,}', ' ', name)
        name = name.strip('. ')
        return name

    async def rename_pending_files(self) -> dict:
        """Process hashed files through the LLM — rename and extract metadata."""
        self._running = True
        stats = {"renamed": 0, "skipped": 0, "errors": 0, "metadata_extracted": 0}

        logger.info("Starting rename pass")
        await self.db.set_job_state("rename_status", "running")

        try:
            while self._running:
                files = await self.db.get_files_by_status("hashed", limit=self.batch_size)
                if not files:
                    break

                filenames = [f["current_name"] for f in files]

                try:
                    suggestions = await self._call_ollama(filenames)
                except Exception as e:
                    logger.error(f"Ollama call failed: {e}")
                    stats["errors"] += len(files)
                    for f in files:
                        await self.db.update_file_status(f["id"], "hashed")
                    break

                # Map suggestions back to files
                suggestion_map = {}
                for s in suggestions:
                    orig = s.get("original", "")
                    if orig:
                        suggestion_map[orig] = s

                for f in files:
                    if not self._running:
                        break

                    file_id = f["id"]
                    current_name = f["current_name"]
                    entry = suggestion_map.get(current_name, {})

                    # Extract and store metadata regardless of rename
                    content_title = entry.get("content_title", "")
                    media_type = entry.get("media_type", "other")
                    resolution = entry.get("resolution")
                    codec = entry.get("codec")
                    source = entry.get("source")
                    quality_score = self._compute_quality_score(resolution, source, codec)

                    if content_title:
                        await self.db.update_file_metadata(
                            file_id, content_title, media_type,
                            resolution, quality_score, codec, source
                        )
                        stats["metadata_extracted"] += 1

                    # Handle rename
                    suggested = entry.get("suggested", "")

                    if not suggested or suggested == current_name:
                        await self.db.update_file_status(file_id, "renamed")
                        stats["skipped"] += 1
                        continue

                    suggested = self._sanitize_filename(suggested)
                    if not suggested:
                        await self.db.update_file_status(file_id, "renamed")
                        stats["skipped"] += 1
                        continue

                    await self.db.update_file_proposed_name(file_id, suggested)

                    if settings.auto_mode:
                        result = await self._execute_rename(f, suggested)
                        if result:
                            stats["renamed"] += 1
                        else:
                            stats["errors"] += 1

        except Exception as e:
            logger.error(f"Rename pass failed: {e}")
            await self.db.set_job_state("rename_status", "error")
            raise
        finally:
            self._running = False

        await self.db.set_job_state("rename_status", "completed")
        logger.info(f"Rename complete: {stats}")
        return stats

    async def _execute_rename(self, file_record: dict, new_name: str) -> bool:
        """Actually rename the file on disk."""
        file_id = file_record["id"]
        old_path = file_record["path"]
        directory = os.path.dirname(old_path)
        new_path = os.path.join(directory, new_name)

        # Avoid overwriting existing files
        if os.path.exists(new_path) and old_path != new_path:
            base, ext = os.path.splitext(new_name)
            counter = 1
            while os.path.exists(new_path):
                new_name = f"{base} ({counter}){ext}"
                new_path = os.path.join(directory, new_name)
                counter += 1

        try:
            if old_path != new_path:
                os.rename(old_path, new_path)
                await self.db.log_rename(
                    file_id, old_path, new_path,
                    file_record["current_name"], new_name
                )
                await self.db.mark_file_renamed(file_id, new_path, new_name)
                logger.info(f"Renamed: {file_record['current_name']} -> {new_name}")
            else:
                await self.db.update_file_status(file_id, "renamed")
            return True
        except OSError as e:
            logger.error(f"Failed to rename {old_path}: {e}")
            await self.db.update_file_status(file_id, "error", str(e))
            return False
