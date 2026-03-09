"""Recursive media file scanner — memory-efficient for large directories."""

import os
import logging
from datetime import datetime, timezone
from app.config import settings
from app.database import Database

logger = logging.getLogger(__name__)


class Scanner:
    def __init__(self, db: Database):
        self.db = db
        self.extensions = set(
            ext.strip().lower() for ext in settings.scan_extensions.split(",") if ext.strip()
        )
        self.media_path = settings.media_path
        self.batch_size = settings.scan_batch_size
        self._running = False
        self._paused = False

    @property
    def running(self) -> bool:
        return self._running

    def stop(self):
        self._running = False

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    async def scan(self) -> dict:
        """Recursively scan the media directory and register all matching files."""
        self._running = True
        self._paused = False
        stats = {"discovered": 0, "skipped": 0, "errors": 0}

        logger.info(f"Starting scan of {self.media_path}")
        await self.db.set_job_state("scan_status", "running")

        try:
            for dirpath, dirnames, filenames in os.walk(self.media_path):
                if not self._running:
                    logger.info("Scan stopped by user")
                    break

                while self._paused:
                    import asyncio
                    await asyncio.sleep(1)
                    if not self._running:
                        break

                # Skip hidden directories and the duplicates staging folder
                dirnames[:] = [
                    d for d in dirnames
                    if not d.startswith(".") and d != settings.duplicates_dir
                ]

                batch = []
                for filename in filenames:
                    if not self._running:
                        break

                    _, ext = os.path.splitext(filename)
                    ext_lower = ext.lower()

                    if ext_lower not in self.extensions:
                        stats["skipped"] += 1
                        continue

                    filepath = os.path.join(dirpath, filename)

                    try:
                        stat = os.stat(filepath)
                        modified_at = datetime.fromtimestamp(
                            stat.st_mtime, tz=timezone.utc
                        ).isoformat()

                        batch.append({
                            "path": filepath,
                            "name": filename,
                            "ext": ext_lower,
                            "size": stat.st_size,
                            "modified_at": modified_at,
                        })

                        if len(batch) >= self.batch_size:
                            await self._flush_batch(batch)
                            stats["discovered"] += len(batch)
                            batch = []

                    except OSError as e:
                        logger.warning(f"Error accessing {filepath}: {e}")
                        stats["errors"] += 1

                # Flush remaining batch for this directory
                if batch:
                    await self._flush_batch(batch)
                    stats["discovered"] += len(batch)

                # Update directory scan progress
                await self.db.update_scan_progress(
                    dirpath, stats["discovered"], stats["discovered"], "scanning"
                )

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            await self.db.set_job_state("scan_status", "error")
            raise
        finally:
            self._running = False

        status = "stopped" if not self._running else "completed"
        await self.db.set_job_state("scan_status", status)
        await self.db.update_scan_progress(
            self.media_path, stats["discovered"], stats["discovered"], "scanned"
        )

        logger.info(f"Scan complete: {stats}")
        return stats

    async def _flush_batch(self, batch: list[dict]):
        """Write a batch of discovered files to the database."""
        for f in batch:
            try:
                await self.db.upsert_file(
                    path=f["path"],
                    name=f["name"],
                    ext=f["ext"],
                    size=f["size"],
                    modified_at=f["modified_at"],
                )
            except Exception as e:
                logger.warning(f"Error upserting {f['path']}: {e}")
