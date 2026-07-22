"""SQLite state: user-id cache, per-account last_seen, seen-id dedup.

Restart-safe. WAL mode so a reader never blocks the writer.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass


@dataclass
class UserRecord:
    handle: str
    rest_id: str
    name: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    handle     TEXT PRIMARY KEY,
    rest_id    TEXT NOT NULL,
    name       TEXT,
    updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS state (
    handle        TEXT PRIMARY KEY,
    last_seen_id  TEXT,
    last_poll_at  INTEGER,
    initialized   INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS seen_tweets (
    tweet_id  TEXT PRIMARY KEY,
    handle    TEXT,
    seen_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_seen_handle ON seen_tweets(handle);
"""


class Store:
    def __init__(self, path: str) -> None:
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    # -- user cache ---------------------------------------------------------
    def get_user(self, handle: str) -> UserRecord | None:
        row = self._db.execute(
            "SELECT handle, rest_id, name FROM users WHERE handle = ?", (handle,)
        ).fetchone()
        if row is None:
            return None
        return UserRecord(row["handle"], row["rest_id"], row["name"] or "")

    def put_user(self, rec: UserRecord) -> None:
        self._db.execute(
            "INSERT INTO users(handle, rest_id, name, updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(handle) DO UPDATE SET rest_id=excluded.rest_id, "
            "name=excluded.name, updated_at=excluded.updated_at",
            (rec.handle, rec.rest_id, rec.name, int(time.time())),
        )
        self._db.commit()

    # -- per-account state --------------------------------------------------
    def get_last_seen(self, handle: str) -> str | None:
        row = self._db.execute(
            "SELECT last_seen_id FROM state WHERE handle = ?", (handle,)
        ).fetchone()
        return row["last_seen_id"] if row else None

    def is_initialized(self, handle: str) -> bool:
        row = self._db.execute(
            "SELECT initialized FROM state WHERE handle = ?", (handle,)
        ).fetchone()
        return bool(row and row["initialized"])

    def set_state(self, handle: str, last_seen_id: str | None, initialized: bool) -> None:
        self._db.execute(
            "INSERT INTO state(handle, last_seen_id, last_poll_at, initialized) "
            "VALUES(?,?,?,?) ON CONFLICT(handle) DO UPDATE SET "
            "last_seen_id=excluded.last_seen_id, last_poll_at=excluded.last_poll_at, "
            "initialized=excluded.initialized",
            (handle, last_seen_id, int(time.time()), int(initialized)),
        )
        self._db.commit()

    # -- dedup --------------------------------------------------------------
    def is_seen(self, tweet_id: str) -> bool:
        return (
            self._db.execute(
                "SELECT 1 FROM seen_tweets WHERE tweet_id = ?", (tweet_id,)
            ).fetchone()
            is not None
        )

    def mark_seen(self, tweet_id: str, handle: str) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO seen_tweets(tweet_id, handle, seen_at) VALUES(?,?,?)",
            (tweet_id, handle, int(time.time())),
        )
        self._db.commit()

    def prune_seen(self, keep_per_handle: int = 400) -> None:
        """Bound the seen table so it doesn't grow forever."""
        self._db.execute(
            """
            DELETE FROM seen_tweets WHERE tweet_id IN (
                SELECT tweet_id FROM (
                    SELECT tweet_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY handle ORDER BY seen_at DESC
                           ) AS rn
                    FROM seen_tweets
                ) WHERE rn > ?
            )
            """,
            (keep_per_handle,),
        )
        self._db.commit()
