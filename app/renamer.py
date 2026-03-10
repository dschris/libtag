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

SYSTEM_PROMPT = """You are a media file naming expert. Given a single media filename, you must:
1. Suggest a clean, descriptive new filename
2. Extract structured metadata about the content

Return a JSON object with these fields:
- "suggested": the clean new filename (keep the same file extension)
- "content_title": normalized content identity (e.g. "Inception (2010)", "Breaking Bad S01E01")
- "media_type": one of "movie", "tv", "music", "other"
- "resolution": e.g. "2160p", "1080p", "720p", "480p", or null
- "codec": e.g. "x265", "x264", "AV1", or null
- "source": e.g. "BluRay", "WEB-DL", "HDTV", "DVDRip", or null

Rules:
- Remove junk: release group tags, scene tags, unnecessary dots/brackets
- Keep the file extension unchanged
- If the name is already clean, return it unchanged as "suggested"
- content_title must be NORMALIZED: same content at different qualities must have the EXACT same content_title
- Include the year in content_title for movies, e.g. "Inception (2010)"
- Include season/episode for TV, e.g. "Breaking Bad S01E01"

Example input: "Inception.2010.1080p.BluRay.x264-GROUP.mkv"
Example output: {"suggested": "Inception (2010) 1080p BluRay.mkv", "content_title": "Inception (2010)", "media_type": "movie", "resolution": "1080p", "codec": "x264", "source": "BluRay"}"""


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

    async def _call_ollama_single(self, filename: str, folder_name: str | None = None) -> dict:
        """Send a single filename to Ollama and parse the response."""
        context = f'\nFilename: "{filename}"'
        if folder_name and folder_name != os.path.basename(settings.media_path):
            context = f'\nFolder: "{folder_name}"\nFilename: "{filename}"\n\nUse BOTH the folder name and filename as context clues to determine the content.'
        prompt = f'{SYSTEM_PROMPT}\n\nAnalyze this media file and return a JSON object:{context}'

        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 1024,
            },
            "format": "json",
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
            )
            response.raise_for_status()

        result = response.json()
        content = result.get("response", "")
        logger.info(f"Ollama response for '{filename}': {content}")

        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list) and len(parsed) > 0:
                return parsed[0] if isinstance(parsed[0], dict) else {}
            return {}
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM response as JSON: {content[:200]}")
            return {}

    def _sanitize_filename(self, name: str) -> str:
        """Remove characters that are illegal in common filesystems."""
        illegal = r'[<>:"/\\|?*\x00-\x1f]'
        name = re.sub(illegal, '', name)
        name = re.sub(r'\.{2,}', '.', name)
        name = re.sub(r'\s{2,}', ' ', name)
        name = name.strip('. ')
        return name

    def _clean_filename(self, name: str) -> str:
        """Deterministic cleanup: title case, remove dashes, clean separators."""
        base, ext = os.path.splitext(name)
        # Replace common separators (dashes, underscores, dots) with spaces
        clean = re.sub(r'[-_.]+', ' ', base)
        # Remove bracketed junk like [GROUP] or {xxx}
        clean = re.sub(r'[\[\{][^\]\}]*[\]\}]', '', clean)
        # Collapse whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        # Title case
        clean = clean.title()
        return f"{clean}{ext}"

    async def rename_pending_files(self) -> dict:
        """Process hashed files through the LLM one at a time."""
        self._running = True
        stats = {"renamed": 0, "skipped": 0, "errors": 0, "metadata_extracted": 0}

        logger.info("Starting rename pass")
        await self.db.set_job_state("rename_status", "running")

        try:
            while self._running:
                files = await self.db.get_files_by_status("hashed", limit=self.batch_size)
                if not files:
                    break

                for f in files:
                    if not self._running:
                        break

                    file_id = f["id"]
                    current_name = f["current_name"]
                    logger.info(f"Processing: {current_name} (in {os.path.basename(os.path.dirname(f['path']))}/)")

                    # Pass parent folder name for additional context
                    folder_name = os.path.basename(os.path.dirname(f["path"]))

                    try:
                        entry = await self._call_ollama_single(current_name, folder_name)
                    except Exception as e:
                        logger.error(f"Ollama call failed for {current_name}: {e}")
                        await self.db.update_file_status(file_id, "error", str(e))
                        stats["errors"] += 1
                        continue

                    # Extract and store metadata
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
                        logger.info(f"  Metadata: title='{content_title}' type={media_type} "
                                    f"res={resolution} codec={codec} source={source} score={quality_score}")

                    # Handle rename — renaming is mandatory
                    suggested = entry.get("suggested", "")

                    # If LLM didn't suggest anything useful, apply deterministic cleanup
                    if not suggested or suggested == current_name:
                        suggested = self._clean_filename(current_name)
                    else:
                        # Even LLM suggestions get post-processed for consistent formatting
                        suggested = self._clean_filename(suggested)

                    suggested = self._sanitize_filename(suggested)

                    # After all cleanup, if the name is truly unchanged, just mark renamed
                    if not suggested or suggested == current_name:
                        logger.info(f"  Already clean: {current_name}")
                        await self.db.update_file_status(file_id, "renamed")
                        stats["skipped"] += 1
                        continue

                    logger.info(f"  Renaming: {current_name} -> {suggested}")
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
        """Rename the file, flatten to media root, and stage leftover folders."""
        file_id = file_record["id"]
        old_path = file_record["path"]
        media_root = settings.media_path
        old_dir = os.path.dirname(old_path)

        # Always place renamed files in the media root (flatten)
        new_path = os.path.join(media_root, new_name)

        # Avoid overwriting existing files
        if os.path.exists(new_path) and old_path != new_path:
            base, ext = os.path.splitext(new_name)
            counter = 1
            while os.path.exists(new_path):
                new_name = f"{base} ({counter}){ext}"
                new_path = os.path.join(media_root, new_name)
                counter += 1

        try:
            if old_path != new_path:
                os.rename(old_path, new_path)
                await self.db.log_rename(
                    file_id, old_path, new_path,
                    file_record["current_name"], new_name
                )
                await self.db.mark_file_renamed(file_id, new_path, new_name)
                logger.info(f"  ✓ Renamed on disk: {new_name}")

                # If the file was in a subdirectory, move leftover folder to staging
                if os.path.normpath(old_dir) != os.path.normpath(media_root):
                    self._stage_leftover_folder(old_dir, media_root)
            else:
                await self.db.update_file_status(file_id, "renamed")
            return True
        except OSError as e:
            logger.error(f"Failed to rename {old_path}: {e}")
            await self.db.update_file_status(file_id, "error", str(e))
            return False

    @staticmethod
    def _stage_leftover_folder(folder: str, media_root: str):
        """Move a leftover folder to the staging area for cleanup."""
        staging_base = os.path.join(media_root, settings.duplicates_dir, "_leftovers")

        # Find the top-level subfolder relative to media root
        rel = os.path.relpath(folder, media_root)
        top_folder = rel.split(os.sep)[0]
        top_folder_path = os.path.join(media_root, top_folder)

        if not os.path.isdir(top_folder_path):
            return

        try:
            # Check if folder is now empty — just delete it
            if not os.listdir(top_folder_path):
                os.rmdir(top_folder_path)
                logger.info(f"  Removed empty dir: {top_folder}")
                return

            # Move the whole folder to staging
            staging_dest = os.path.join(staging_base, top_folder)
            os.makedirs(staging_base, exist_ok=True)

            # Handle existing name in staging
            if os.path.exists(staging_dest):
                counter = 1
                while os.path.exists(f"{staging_dest} ({counter})"):
                    counter += 1
                staging_dest = f"{staging_dest} ({counter})"

            import shutil
            shutil.move(top_folder_path, staging_dest)
            logger.info(f"  Staged leftover folder: {top_folder} -> {staging_dest}")
        except OSError as e:
            logger.warning(f"  Failed to stage folder {top_folder}: {e}")

