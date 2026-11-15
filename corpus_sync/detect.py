# -*- coding: utf-8 -*-
"""Doc/field/sentence change detection between two corpus snapshots (T7).

Compares the normalized record hashes of an old and a new snapshot:

- NEW      present in new, absent in old
- MODIFIED present in both, any of {arabic, english, metadata} hash differs
  (record_hash is the union of those, so a differing record_hash always
  maps to a non-empty changed_fields list)
- REMOVED  present in old, absent in new  — reported ONLY when BOTH
  snapshots are 'complete' (a partial snapshot may simply be missing docs
  it had not fetched yet; guessing removals from it would be a false
  positive). In that case IncompleteSnapshotError is raised BEFORE any
  row is written.
- UNCHANGED present in both, all hashes equal — counted, NOT stored.

MODIFIED docs additionally get ``sentence_stats`` via
``diff.sentence_diff`` on the raw field texts, loaded lazily from both
snapshots' raw JSONL by source_id. Results are persisted with
``store.record_changes`` (one row per NEW/MODIFIED/REMOVED doc).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import sqlite3

from . import diff, store

# (record column, reported field name) — in the order the spec lists them.
_FIELD_HASHES = (
    ("arabic_hash", "arabic"),
    ("english_hash", "english"),
    ("metadata_hash", "metadata"),
)


class IncompleteSnapshotError(RuntimeError):
    """Raised when change detection would run over a non-complete snapshot."""

    def __init__(self, old_snap_id: str, new_snap_id: str):
        super().__init__(
            f"change detection requires both snapshots complete; "
            f"old={old_snap_id!r}, new={new_snap_id!r}"
        )
        self.old_snap_id = old_snap_id
        self.new_snap_id = new_snap_id


@dataclass
class ChangeSet:
    new: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    unchanged: int = 0
    removed: list[str] = field(default_factory=list)
    details: dict[str, dict] = field(default_factory=dict)


def previous_complete(conn: sqlite3.Connection, snap_id: str) -> str | None:
    """Newest complete snapshot strictly older than ``snap_id`` (or None)."""
    row = conn.execute(
        "SELECT id FROM snapshots WHERE status='complete' AND id<?"
        " ORDER BY id DESC LIMIT 1", (snap_id,)
    ).fetchone()
    return row[0] if row else None


def load_raw_index(raw_path: Path) -> dict[str, dict]:
    """source_id -> raw doc, one lazy line scan of a snapshot raw JSONL."""
    out: dict[str, dict] = {}
    with open(raw_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            out[rec["source_id"]] = rec["raw"]
    return out


def _sentence_stats(old_raw: dict, new_raw: dict) -> dict:
    stats = {}
    for lang, key in (("ar", "arabic"), ("en", "english")):
        d = diff.sentence_diff(old_raw.get(key) or "",
                               new_raw.get(key) or "", lang)
        stats[lang] = {"added": d.added, "removed": d.removed,
                       "modified": d.modified}
    return stats


def detect_changes(conn: sqlite3.Connection, *, old_snap_id: str,
                   new_snap_id: str) -> ChangeSet:
    """Diff two snapshots and persist NEW/MODIFIED/REMOVED rows."""
    old_status = store.snapshot_status(conn, old_snap_id)
    new_status = store.snapshot_status(conn, new_snap_id)
    if old_status != "complete" or new_status != "complete":
        # Raise before touching the raw files or writing any row.
        raise IncompleteSnapshotError(old_snap_id, new_snap_id)

    old_hashes = store.load_record_hashes(conn, old_snap_id)
    new_hashes = store.load_record_hashes(conn, new_snap_id)
    old_raw = load_raw_index(store.resolve_raw_path(
        conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                     (old_snap_id,)).fetchone()[0]))
    new_raw = load_raw_index(store.resolve_raw_path(
        conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                     (new_snap_id,)).fetchone()[0]))

    cs = ChangeSet()
    rows: list[dict] = []

    for sid in sorted(new_hashes):
        if sid not in old_hashes:
            cs.new.append(sid)
            rows.append({"source_id": sid, "doc_status": "NEW",
                         "changed_fields": None, "sentence_stats": None})
            continue
        o, n = old_hashes[sid], new_hashes[sid]
        changed = [name for col, name in _FIELD_HASHES
                   if o[col] != n[col]]
        if not changed:
            cs.unchanged += 1
            continue
        cs.modified.append(sid)
        stats = _sentence_stats(old_raw[sid], new_raw[sid])
        cs.details[sid] = {"changed_fields": changed,
                           "sentence_stats": stats}
        rows.append({"source_id": sid, "doc_status": "MODIFIED",
                     "changed_fields": changed, "sentence_stats": stats})

    for sid in sorted(old_hashes):
        if sid not in new_hashes:
            cs.removed.append(sid)
            rows.append({"source_id": sid, "doc_status": "REMOVED",
                         "changed_fields": None, "sentence_stats": None})

    store.record_changes(conn, new_snap_id, rows)
    return cs
