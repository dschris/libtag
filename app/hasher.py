"""Fast file hashing engine using xxHash for deduplication."""

import os
import logging
import xxhash
from app.config import settings
from app.database import Database

logger = logging.getLogger(__name__)


class Hasher:
    def __init__(self, db: Database):
        self.db = db
        self.chunk_size = settings.hash_chunk_size
        self.use_partial = settings.use_partial_hash
        self.partial_size = settings.partial_hash_size
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def stop(self):
        self._running = False

    def _compute_full_hash(self, filepath: str) -> str:
        """Compute full xxHash-128 of a file."""
        h = xxhash.xxh128()
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def _compute_partial_hash(self, filepath: str, file_size: int) -> str:
        """Compute hash of first + last N bytes for fast initial comparison."""
        h = xxhash.xxh128()
        with open(filepath, "rb") as f:
            # Read first chunk
            head = f.read(min(self.partial_size, file_size))
            h.update(head)

            # Read last chunk if file is large enough
            if file_size > self.partial_size * 2:
                f.seek(-self.partial_size, 2)
                tail = f.read(self.partial_size)
                h.update(tail)

        # Include file size in the hash for extra discrimination
        h.update(str(file_size).encode())
        return h.hexdigest()

    async def hash_pending_files(self) -> dict:
        """Hash all scanned files that haven't been hashed yet."""
        self._running = True
        stats = {"hashed": 0, "errors": 0}

        logger.info("Starting hashing pass")
        await self.db.set_job_state("hash_status", "running")

        try:
            while self._running:
                files = await self.db.get_files_by_status("scanned", limit=50)
                if not files:
                    break

                for f in files:
                    if not self._running:
                        break

                    file_id = f["id"]
                    filepath = f["path"]

                    try:
                        if not os.path.exists(filepath):
                            await self.db.update_file_status(file_id, "error", "File not found")
                            stats["errors"] += 1
                            continue

                        await self.db.update_file_status(file_id, "hashing")

                        file_size = f["size"]

                        if self.use_partial and file_size > self.partial_size * 2:
                            # For large files, compute partial hash first
                            partial = self._compute_partial_hash(filepath, file_size)
                            full = self._compute_full_hash(filepath)
                            await self.db.update_file_hash(file_id, full, partial)
                        else:
                            full = self._compute_full_hash(filepath)
                            await self.db.update_file_hash(file_id, full)

                        stats["hashed"] += 1

                        if stats["hashed"] % 100 == 0:
                            logger.info(f"Hashed {stats['hashed']} files so far")

                    except OSError as e:
                        logger.warning(f"Error hashing {filepath}: {e}")
                        await self.db.update_file_status(file_id, "error", str(e))
                        stats["errors"] += 1

        except Exception as e:
            logger.error(f"Hashing failed: {e}")
            await self.db.set_job_state("hash_status", "error")
            raise
        finally:
            self._running = False

        await self.db.set_job_state("hash_status", "completed")
        logger.info(f"Hashing complete: {stats}")
        return stats

    async def find_and_group_duplicates(self) -> dict:
        """Identify duplicate files and create duplicate groups."""
        stats = {"groups": 0, "duplicate_files": 0, "bytes_recoverable": 0}

        dup_hashes = await self.db.find_duplicate_hashes()
        logger.info(f"Found {len(dup_hashes)} potential duplicate groups")

        for dup in dup_hashes:
            hash_val = dup["hash"]
            files = await self.db.get_files_by_hash(hash_val)

            if len(files) < 2:
                continue

            # Pick the keeper: prefer the one with the longest/most descriptive name
            keeper = max(files, key=lambda f: len(f["current_name"]))
            keeper_id = keeper["id"]

            await self.db.create_duplicate_group(
                hash_val, keeper_id, len(files), dup["total_size"]
            )

            # Mark non-keepers as duplicates
            for f in files:
                if f["id"] != keeper_id:
                    await self.db.mark_file_duplicate(f["id"])
                    stats["duplicate_files"] += 1
                    stats["bytes_recoverable"] += f["size"]

            stats["groups"] += 1

        logger.info(f"Dedup complete: {stats}")
        return stats

    async def move_duplicates_to_staging(self) -> dict:
        """Move duplicate files to the staging directory."""
        stats = {"moved": 0, "errors": 0}
        staging_base = os.path.join(settings.media_path, settings.duplicates_dir)

        dup_files = await self.db.get_files_by_status("duplicate", limit=1000)

        for f in dup_files:
            filepath = f["path"]
            try:
                if not os.path.exists(filepath):
                    await self.db.update_file_status(f["id"], "error", "File not found for move")
                    stats["errors"] += 1
                    continue

                # Preserve relative path structure in staging folder
                rel_path = os.path.relpath(filepath, settings.media_path)
                staging_path = os.path.join(staging_base, rel_path)
                os.makedirs(os.path.dirname(staging_path), exist_ok=True)

                os.rename(filepath, staging_path)
                await self.db.mark_file_renamed(f["id"], staging_path, f["current_name"])
                await self.db.log_rename(
                    f["id"], filepath, staging_path, f["current_name"], f["current_name"]
                )
                stats["moved"] += 1

            except OSError as e:
                logger.warning(f"Error moving duplicate {filepath}: {e}")
                await self.db.update_file_status(f["id"], "error", str(e))
                stats["errors"] += 1

        logger.info(f"Duplicate staging complete: {stats}")
        return stats
