from __future__ import annotations

import sqlite3
from pathlib import Path


def connect_db(sqlite_path: Path) -> sqlite3.Connection:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            filename TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            page_count INTEGER NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pages (
            doc_id INTEGER NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
            page_index INTEGER NOT NULL,
            text TEXT NOT NULL,
            PRIMARY KEY (doc_id, page_index)
        );

        CREATE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256);
        """
    )
