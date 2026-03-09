"""Smart semantic deduplication — identifies same content at different qualities."""

import os
import logging
from app.config import settings
from app.database import Database

logger = logging.getLogger(__name__)


class SmartDedup:
    """Finds files that are the same content at different quality levels.

    After the LLM renamer extracts metadata (content_title, resolution, quality_score, etc.),
    this module groups files by content_title and keeps only the highest-quality version,
    moving lower-quality copies to the staging folder.
    """

    def __init__(self, db: Database):
        self.db = db
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def stop(self):
        self._running = False

    async def find_and_resolve_smart_duplicates(self) -> dict:
        """Find same-content files at different qualities, keep the best, stage the rest."""
        self._running = True
        stats = {
            "groups_found": 0,
            "files_staged": 0,
            "space_saved": 0,
            "errors": 0,
        }

        logger.info("Starting smart deduplication pass")
        await self.db.set_job_state("smart_dedup_status", "running")

        try:
            # Find all content titles that appear more than once
            dup_titles = await self.db.find_semantic_duplicate_titles()
            logger.info(f"Found {len(dup_titles)} potential semantic duplicate groups")

            for title_info in dup_titles:
                if not self._running:
                    break

                content_title = title_info["content_title"]
                media_type = title_info["media_type"]

                # Get all files with this content title, sorted by quality_score DESC
                files = await self.db.get_files_by_content_title(content_title, media_type)

                if len(files) < 2:
                    continue

                # The first file (highest quality_score) is the keeper
                keeper = files[0]
                keeper_id = keeper["id"]
                losers = files[1:]

                # Calculate space that will be freed
                space_saved = sum(f["size"] for f in losers)
                total_size = sum(f["size"] for f in files)

                logger.info(
                    f"Smart dedup: '{content_title}' — keeping {keeper['current_name']} "
                    f"(score={keeper['quality_score']}, res={keeper.get('resolution')}), "
                    f"staging {len(losers)} lower-quality copies"
                )

                # Create the semantic duplicate group record
                await self.db.create_semantic_duplicate_group(
                    content_title=content_title,
                    media_type=media_type,
                    keeper_id=keeper_id,
                    count=len(files),
                    total_size=total_size,
                    space_saved=space_saved,
                )

                # Mark losers and move them to staging
                for loser in losers:
                    if not self._running:
                        break

                    await self.db.mark_file_smart_duplicate(loser["id"])
                    moved = await self._move_to_staging(loser)
                    if moved:
                        stats["files_staged"] += 1
                        stats["space_saved"] += loser["size"]
                    else:
                        stats["errors"] += 1

                stats["groups_found"] += 1

        except Exception as e:
            logger.error(f"Smart dedup failed: {e}")
            await self.db.set_job_state("smart_dedup_status", "error")
            raise
        finally:
            self._running = False

        await self.db.set_job_state("smart_dedup_status", "completed")
        logger.info(
            f"Smart dedup complete: {stats['groups_found']} groups, "
            f"{stats['files_staged']} files staged, "
            f"{self._format_bytes(stats['space_saved'])} recoverable"
        )
        return stats

    async def _move_to_staging(self, file_record: dict) -> bool:
        """Move a lower-quality duplicate to the staging directory."""
        filepath = file_record["path"]
        staging_base = os.path.join(settings.media_path, settings.duplicates_dir, "_smart_dedup")

        try:
            if not os.path.exists(filepath):
                await self.db.update_file_status(
                    file_record["id"], "error", "File not found for smart dedup move"
                )
                return False

            # Preserve relative path structure in staging folder
            rel_path = os.path.relpath(filepath, settings.media_path)
            staging_path = os.path.join(staging_base, rel_path)
            os.makedirs(os.path.dirname(staging_path), exist_ok=True)

            os.rename(filepath, staging_path)

            await self.db.log_rename(
                file_record["id"], filepath, staging_path,
                file_record["current_name"], file_record["current_name"]
            )
            await self.db.mark_file_renamed(file_record["id"], staging_path, file_record["current_name"])

            logger.info(
                f"  Staged: {file_record['current_name']} "
                f"(score={file_record.get('quality_score', '?')}, "
                f"res={file_record.get('resolution', '?')})"
            )
            return True

        except OSError as e:
            logger.warning(f"Error moving smart duplicate {filepath}: {e}")
            await self.db.update_file_status(file_record["id"], "error", str(e))
            return False

    @staticmethod
    def _format_bytes(size: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"
