"""SQLite database layer for tracking files, hashes, renames, and duplicates."""

import aiosqlite
import os
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    directory TEXT NOT NULL,
    original_name TEXT NOT NULL,
    proposed_name TEXT,
    current_name TEXT NOT NULL,
    extension TEXT NOT NULL,
    size INTEGER NOT NULL,
    modified_at TEXT NOT NULL,
    hash TEXT,
    partial_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    -- status: pending | scanned | hashing | hashed | renaming | renamed | duplicate | smart_duplicate | error
    error_message TEXT,
    -- Structured metadata extracted by LLM for semantic dedup
    content_title TEXT,       -- normalized title (e.g. "Inception", "Breaking Bad S01E01")
    media_type TEXT,          -- movie | tv | music | other
    resolution TEXT,          -- e.g. 2160p, 1080p, 720p, 480p, SD
    quality_score INTEGER,    -- computed quality rank (higher = better)
    codec TEXT,               -- e.g. x265, x264, av1
    source TEXT,              -- e.g. BluRay, WEB-DL, HDTV, DVDRip
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(path)
);

CREATE TABLE IF NOT EXISTS duplicate_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT NOT NULL UNIQUE,
    keeper_file_id INTEGER,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    resolved BOOLEAN NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (keeper_file_id) REFERENCES files(id)
);

CREATE TABLE IF NOT EXISTS semantic_duplicate_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_title TEXT NOT NULL,
    media_type TEXT,
    keeper_file_id INTEGER,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    space_saved INTEGER NOT NULL DEFAULT 0,
    resolved BOOLEAN NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (keeper_file_id) REFERENCES files(id)
);

CREATE TABLE IF NOT EXISTS rename_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    old_path TEXT NOT NULL,
    new_path TEXT NOT NULL,
    old_name TEXT NOT NULL,
    new_name TEXT NOT NULL,
    undone BOOLEAN NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (file_id) REFERENCES files(id)
);

CREATE TABLE IF NOT EXISTS scan_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory TEXT NOT NULL UNIQUE,
    total_files INTEGER NOT NULL DEFAULT 0,
    processed_files INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    -- status: pending | scanning | scanned | error
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS job_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_partial_hash ON files(partial_hash);
CREATE INDEX IF NOT EXISTS idx_files_directory ON files(directory);
CREATE INDEX IF NOT EXISTS idx_files_content_title ON files(content_title);
CREATE INDEX IF NOT EXISTS idx_rename_log_file_id ON rename_log(file_id);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Database not connected"
        return self._db

    # ── File operations ──────────────────────────────────────────────

    async def upsert_file(self, path: str, name: str, ext: str, size: int, modified_at: str) -> int:
        directory = os.path.dirname(path)
        await self.db.execute(
            """INSERT INTO files (path, directory, original_name, current_name, extension, size, modified_at, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'scanned')
               ON CONFLICT(path) DO UPDATE SET
                   size=excluded.size, modified_at=excluded.modified_at, updated_at=datetime('now')""",
            (path, directory, name, name, ext, size, modified_at),
        )
        await self.db.commit()
        cursor = await self.db.execute("SELECT id FROM files WHERE path = ?", (path,))
        row = await cursor.fetchone()
        return row["id"]

    async def get_files_by_status(self, status: str, limit: int = 100) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM files WHERE status = ? ORDER BY id LIMIT ?", (status, limit)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def update_file_hash(self, file_id: int, hash_val: str, partial_hash: str | None = None):
        await self.db.execute(
            "UPDATE files SET hash = ?, partial_hash = ?, status = 'hashed', updated_at = datetime('now') WHERE id = ?",
            (hash_val, partial_hash, file_id),
        )
        await self.db.commit()

    async def update_file_partial_hash(self, file_id: int, partial_hash: str):
        await self.db.execute(
            "UPDATE files SET partial_hash = ?, status = 'hashed', updated_at = datetime('now') WHERE id = ?",
            (partial_hash, file_id),
        )
        await self.db.commit()

    async def update_file_status(self, file_id: int, status: str, error: str | None = None):
        await self.db.execute(
            "UPDATE files SET status = ?, error_message = ?, updated_at = datetime('now') WHERE id = ?",
            (status, error, file_id),
        )
        await self.db.commit()

    async def update_file_proposed_name(self, file_id: int, proposed_name: str):
        await self.db.execute(
            "UPDATE files SET proposed_name = ?, status = 'renaming', updated_at = datetime('now') WHERE id = ?",
            (proposed_name, file_id),
        )
        await self.db.commit()

    async def mark_file_renamed(self, file_id: int, new_path: str, new_name: str):
        await self.db.execute(
            "UPDATE files SET path = ?, current_name = ?, status = 'renamed', updated_at = datetime('now') WHERE id = ?",
            (new_path, new_name, file_id),
        )
        await self.db.commit()

    async def mark_file_duplicate(self, file_id: int):
        await self.db.execute(
            "UPDATE files SET status = 'duplicate', updated_at = datetime('now') WHERE id = ?",
            (file_id,),
        )
        await self.db.commit()

    async def mark_file_smart_duplicate(self, file_id: int):
        await self.db.execute(
            "UPDATE files SET status = 'smart_duplicate', updated_at = datetime('now') WHERE id = ?",
            (file_id,),
        )
        await self.db.commit()

    async def update_file_metadata(self, file_id: int, content_title: str, media_type: str,
                                    resolution: str | None, quality_score: int,
                                    codec: str | None, source: str | None):
        await self.db.execute(
            """UPDATE files SET content_title = ?, media_type = ?, resolution = ?,
               quality_score = ?, codec = ?, source = ?, updated_at = datetime('now') WHERE id = ?""",
            (content_title, media_type, resolution, quality_score, codec, source, file_id),
        )
        await self.db.commit()

    # ── Duplicate operations ─────────────────────────────────────────

    async def find_duplicate_hashes(self) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT hash, COUNT(*) as cnt, SUM(size) as total_size
               FROM files WHERE hash IS NOT NULL AND status NOT IN ('duplicate', 'smart_duplicate')
               GROUP BY hash HAVING cnt > 1"""
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_files_by_hash(self, hash_val: str) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM files WHERE hash = ? ORDER BY id", (hash_val,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def create_duplicate_group(self, hash_val: str, keeper_id: int, count: int, total_size: int):
        await self.db.execute(
            """INSERT INTO duplicate_groups (hash, keeper_file_id, file_count, total_size)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(hash) DO UPDATE SET
                   keeper_file_id=excluded.keeper_file_id, file_count=excluded.file_count,
                   total_size=excluded.total_size""",
            (hash_val, keeper_id, count, total_size),
        )
        await self.db.commit()

    # ── Semantic duplicate operations ────────────────────────────────

    async def find_semantic_duplicate_titles(self) -> list[dict]:
        """Find content titles that appear more than once (potential semantic dups)."""
        cursor = await self.db.execute(
            """SELECT content_title, media_type, COUNT(*) as cnt, SUM(size) as total_size
               FROM files
               WHERE content_title IS NOT NULL
                 AND content_title != ''
                 AND status NOT IN ('duplicate', 'smart_duplicate', 'error')
               GROUP BY content_title, media_type
               HAVING cnt > 1"""
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_files_by_content_title(self, content_title: str, media_type: str | None = None) -> list[dict]:
        if media_type:
            cursor = await self.db.execute(
                """SELECT * FROM files
                   WHERE content_title = ? AND media_type = ?
                     AND status NOT IN ('duplicate', 'smart_duplicate', 'error')
                   ORDER BY quality_score DESC, size DESC""",
                (content_title, media_type),
            )
        else:
            cursor = await self.db.execute(
                """SELECT * FROM files
                   WHERE content_title = ?
                     AND status NOT IN ('duplicate', 'smart_duplicate', 'error')
                   ORDER BY quality_score DESC, size DESC""",
                (content_title,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def create_semantic_duplicate_group(self, content_title: str, media_type: str | None,
                                              keeper_id: int, count: int,
                                              total_size: int, space_saved: int):
        await self.db.execute(
            """INSERT INTO semantic_duplicate_groups
               (content_title, media_type, keeper_file_id, file_count, total_size, space_saved)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (content_title, media_type, keeper_id, count, total_size, space_saved),
        )
        await self.db.commit()

    async def get_semantic_duplicate_groups(self) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM semantic_duplicate_groups WHERE resolved = 0 ORDER BY space_saved DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Rename log ───────────────────────────────────────────────────

    async def log_rename(self, file_id: int, old_path: str, new_path: str, old_name: str, new_name: str):
        await self.db.execute(
            "INSERT INTO rename_log (file_id, old_path, new_path, old_name, new_name) VALUES (?, ?, ?, ?, ?)",
            (file_id, old_path, new_path, old_name, new_name),
        )
        await self.db.commit()

    async def get_rename_history(self, limit: int = 100, offset: int = 0) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM rename_log ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def undo_rename(self, log_id: int) -> dict | None:
        cursor = await self.db.execute(
            "SELECT * FROM rename_log WHERE id = ? AND undone = 0", (log_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        entry = dict(row)
        await self.db.execute("UPDATE rename_log SET undone = 1 WHERE id = ?", (log_id,))
        await self.db.execute(
            "UPDATE files SET path = ?, current_name = ?, status = 'hashed', updated_at = datetime('now') WHERE id = ?",
            (entry["old_path"], entry["old_name"], entry["file_id"]),
        )
        await self.db.commit()
        return entry

    # ── Scan progress ────────────────────────────────────────────────

    async def update_scan_progress(self, directory: str, total: int, processed: int, status: str):
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO scan_progress (directory, total_files, processed_files, status, started_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(directory) DO UPDATE SET
                   total_files=excluded.total_files, processed_files=excluded.processed_files,
                   status=excluded.status, updated_at=excluded.updated_at,
                   completed_at = CASE WHEN excluded.status = 'scanned' THEN excluded.updated_at ELSE completed_at END""",
            (directory, total, processed, status, now, now),
        )
        await self.db.commit()

    # ── Stats ────────────────────────────────────────────────────────

    async def get_stats(self) -> dict:
        stats = {}
        for status in ["pending", "scanned", "hashing", "hashed", "renaming", "renamed", "duplicate", "smart_duplicate", "error"]:
            cursor = await self.db.execute("SELECT COUNT(*) as c FROM files WHERE status = ?", (status,))
            row = await cursor.fetchone()
            stats[status] = row["c"]

        cursor = await self.db.execute("SELECT COUNT(*) as c FROM files")
        row = await cursor.fetchone()
        stats["total"] = row["c"]

        cursor = await self.db.execute("SELECT COUNT(*) as c FROM duplicate_groups")
        row = await cursor.fetchone()
        stats["duplicate_groups"] = row["c"]

        cursor = await self.db.execute("SELECT COALESCE(SUM(total_size), 0) as s FROM duplicate_groups")
        row = await cursor.fetchone()
        stats["duplicate_size_bytes"] = row["s"]

        cursor = await self.db.execute("SELECT COUNT(*) as c FROM semantic_duplicate_groups")
        row = await cursor.fetchone()
        stats["smart_duplicate_groups"] = row["c"]

        cursor = await self.db.execute("SELECT COALESCE(SUM(space_saved), 0) as s FROM semantic_duplicate_groups")
        row = await cursor.fetchone()
        stats["smart_duplicate_saved_bytes"] = row["s"]

        cursor = await self.db.execute("SELECT COUNT(*) as c FROM rename_log WHERE undone = 0")
        row = await cursor.fetchone()
        stats["renames_completed"] = row["c"]

        return stats

    # ── Job state ────────────────────────────────────────────────────

    async def set_job_state(self, key: str, value: str):
        await self.db.execute(
            "INSERT INTO job_state (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')",
            (key, value),
        )
        await self.db.commit()

    async def get_job_state(self, key: str) -> str | None:
        cursor = await self.db.execute("SELECT value FROM job_state WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row["value"] if row else None

    async def get_all_files(self, limit: int = 100, offset: int = 0, status_filter: str | None = None) -> list[dict]:
        if status_filter:
            cursor = await self.db.execute(
                "SELECT * FROM files WHERE status = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (status_filter, limit, offset),
            )
        else:
            cursor = await self.db.execute(
                "SELECT * FROM files ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_duplicate_groups(self) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM duplicate_groups WHERE resolved = 0 ORDER BY total_size DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
