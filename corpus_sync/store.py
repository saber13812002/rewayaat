# -*- coding: utf-8 -*-
"""SQLite persistence for snapshots, normalized record hashes, and changes.

Layout (single file at ``config.DB_PATH``):

- ``snapshots``  one row per snapshot run (status 'complete'|'partial',
  raw JSONL path, per-book counts as JSON).
- ``records``    one row per (snapshot, doc) with the v1.0 hashes — enough
  to compute doc-level change status without re-reading raw files.
- ``changes``    one row per (snapshot, doc) with the detected status and
  per-language sentence stats (T7 fills it).

All public functions take an explicit ``conn`` so callers own the
transaction/connection lifecycle.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots(
  id TEXT PRIMARY KEY,              -- YYYYMMDD-HHMMSS
  created_at TEXT NOT NULL,         -- iso8601
  status TEXT NOT NULL,             -- 'complete' | 'partial'
  total_docs INTEGER NOT NULL,
  raw_path TEXT NOT NULL,
  book_counts TEXT NOT NULL);       -- json {book: count}
CREATE TABLE IF NOT EXISTS records(
  snapshot_id TEXT NOT NULL, source_id TEXT NOT NULL,
  book TEXT NOT NULL, chapter TEXT NOT NULL, hadith_number TEXT NOT NULL,
  record_hash TEXT NOT NULL, arabic_hash TEXT NOT NULL, english_hash TEXT NOT NULL,
  metadata_hash TEXT NOT NULL, fetched_at TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, source_id));
CREATE TABLE IF NOT EXISTS changes(
  snapshot_id TEXT NOT NULL, source_id TEXT NOT NULL,
  doc_status TEXT NOT NULL,         -- NEW|MODIFIED|UNCHANGED|REMOVED
  changed_fields TEXT,              -- json list
  sentence_stats TEXT,              -- json {"ar":{added,removed,modified},"en":{...}}
  PRIMARY KEY (snapshot_id, source_id));
CREATE TABLE IF NOT EXISTS sentence_totals(
  snapshot_id TEXT NOT NULL, lang TEXT NOT NULL,  -- 'ar' | 'en'
  total INTEGER NOT NULL,
  PRIMARY KEY (snapshot_id, lang));
"""


def open_db(path: Path | None = None) -> sqlite3.Connection:
    """Open (creating if needed) the store DB with WAL journaling."""
    path = path or config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def record_snapshot(conn: sqlite3.Connection, *, snap_id: str, created_at: str,
                    status: str, total_docs: int, raw_path: str,
                    book_counts: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO snapshots(id, created_at, status, total_docs,"
        " raw_path, book_counts) VALUES(?,?,?,?,?,?)",
        (snap_id, created_at, status, total_docs, raw_path,
         json.dumps(book_counts, ensure_ascii=False, sort_keys=True)),
    )
    conn.commit()


def resolve_raw_path(raw_path: str) -> Path:
    """Absolute raw_path as stored, or relative to ``config.DATA_DIR``.

    Relative paths keep the state portable (GitHub Actions restores the
    artifact under a different absolute checkout than the one that wrote
    it); absolute paths (legacy rows, local runs) pass through unchanged.
    """
    p = Path(raw_path)
    return p if p.is_absolute() else config.DATA_DIR / p


def snapshot_status(conn: sqlite3.Connection, snap_id: str) -> str | None:
    row = conn.execute(
        "SELECT status FROM snapshots WHERE id=?", (snap_id,)).fetchone()
    return row[0] if row else None


def latest_snapshot(conn: sqlite3.Connection, *, complete_only: bool = True) -> str | None:
    """Newest snapshot id (lexicographic = chronological for YYYYMMDD-HHMMSS)."""
    if complete_only:
        row = conn.execute(
            "SELECT id FROM snapshots WHERE status='complete' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    else:
        row = conn.execute("SELECT id FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
    return row[0] if row else None


def latest_snapshot_before(conn: sqlite3.Connection, snap_id: str) -> str | None:
    """Newest complete snapshot strictly older than ``snap_id`` (or None)."""
    row = conn.execute(
        "SELECT id FROM snapshots WHERE status='complete' AND id<?"
        " ORDER BY id DESC LIMIT 1", (snap_id,)
    ).fetchone()
    return row[0] if row else None


def insert_records(conn: sqlite3.Connection, snap_id: str, rows: Iterable[dict]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO records(snapshot_id, source_id, book, chapter,"
        " hadith_number, record_hash, arabic_hash, english_hash, metadata_hash,"
        " fetched_at) VALUES(:snapshot_id,:source_id,:book,:chapter,"
        ":hadith_number,:record_hash,:arabic_hash,:english_hash,:metadata_hash,"
        ":fetched_at)",
        [dict(r, snapshot_id=snap_id) for r in rows],
    )
    conn.commit()


def load_record_hashes(conn: sqlite3.Connection, snap_id: str) -> dict[str, dict]:
    """source_id -> {record_hash, arabic_hash, english_hash, metadata_hash, book}."""
    out: dict[str, dict] = {}
    for sid, book, rh, ah, eh, mh in conn.execute(
        "SELECT source_id, book, record_hash, arabic_hash, english_hash,"
        " metadata_hash FROM records WHERE snapshot_id=?", (snap_id,)
    ):
        out[sid] = {"record_hash": rh, "arabic_hash": ah,
                    "english_hash": eh, "metadata_hash": mh, "book": book}
    return out


def record_changes(conn: sqlite3.Connection, snap_id: str, rows: list[dict]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO changes(snapshot_id, source_id, doc_status,"
        " changed_fields, sentence_stats) VALUES(:snapshot_id,:source_id,"
        ":doc_status,:changed_fields,:sentence_stats)",
        [dict(r, snapshot_id=snap_id,
              changed_fields=(json.dumps(r["changed_fields"], ensure_ascii=False)
                              if r.get("changed_fields") is not None else None),
              sentence_stats=(json.dumps(r["sentence_stats"], ensure_ascii=False)
                              if r.get("sentence_stats") is not None else None))
         for r in rows],
    )
    conn.commit()
