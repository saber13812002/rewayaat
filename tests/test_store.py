# -*- coding: utf-8 -*-
"""Tests for corpus_sync.store (SQLite snapshots/records/changes)."""
import json

import pytest

from corpus_sync import store


@pytest.fixture
def conn(tmp_data_dir):
    c = store.open_db()
    try:
        yield c
    finally:
        c.close()


def _record(snap, sid, book="B", chapter="C", num="1",
            rh="h1", ah="h2", eh="h3", mh="h4", fetched="t"):
    return {"snapshot_id": snap, "source_id": sid, "book": book, "chapter": chapter,
            "hadith_number": num, "record_hash": rh, "arabic_hash": ah,
            "english_hash": eh, "metadata_hash": mh, "fetched_at": fetched}


def test_open_db_creates_schema(tmp_data_dir):
    c = store.open_db()
    tables = {r[0] for r in c.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"snapshots", "records", "changes"} <= tables
    wal = c.execute("PRAGMA journal_mode").fetchone()[0]
    assert wal == "wal"
    c.close()


def test_latest_snapshot_prefers_complete_and_order(conn):
    store.record_snapshot(conn, snap_id="20260101-000000", created_at="2026-01-01T00:00:00Z",
                          status="complete", total_docs=10, raw_path="a.jsonl",
                          book_counts={"B": 10})
    store.record_snapshot(conn, snap_id="20260102-000000", created_at="2026-01-02T00:00:00Z",
                          status="partial", total_docs=9, raw_path="b.jsonl",
                          book_counts={"B": 9})
    store.record_snapshot(conn, snap_id="20260103-000000", created_at="2026-01-03T00:00:00Z",
                          status="complete", total_docs=11, raw_path="c.jsonl",
                          book_counts={"B": 11})
    assert store.latest_snapshot(conn, complete_only=True) == "20260103-000000"
    assert store.latest_snapshot(conn, complete_only=False) == "20260103-000000"
    # only partial exists when the complete ones are ignored
    store.record_snapshot(conn, snap_id="20260104-000000", created_at="2026-01-04T00:00:00Z",
                          status="partial", total_docs=1, raw_path="d.jsonl",
                          book_counts={"B": 1})
    assert store.latest_snapshot(conn, complete_only=True) == "20260103-000000"
    assert store.latest_snapshot(conn, complete_only=False) == "20260104-000000"
    assert store.snapshot_status(conn, "20260102-000000") == "partial"


def test_latest_snapshot_none_when_empty(conn):
    assert store.latest_snapshot(conn) is None


def test_insert_records_and_load_hashes_roundtrip(conn):
    store.record_snapshot(conn, snap_id="20260101-000000", created_at="2026-01-01T00:00:00Z",
                          status="complete", total_docs=2, raw_path="a.jsonl",
                          book_counts={"B": 2})
    rows = [
        _record("20260101-000000", "B:1", rh="r1", ah="a1", eh="e1", mh="m1"),
        _record("20260101-000000", "B:2", book="A", chapter="", num="7",
                rh="r2", ah="a2", eh="e2", mh="m2", fetched="t2"),
    ]
    store.insert_records(conn, "20260101-000000", rows)
    got = store.load_record_hashes(conn, "20260101-000000")
    assert set(got) == {"B:1", "B:2"}
    assert got["B:1"] == {"record_hash": "r1", "arabic_hash": "a1",
                          "english_hash": "e1", "metadata_hash": "m1", "book": "B"}
    assert got["B:2"]["book"] == "A"
    assert store.load_record_hashes(conn, "missing") == {}


def test_insert_records_idempotent_replace(conn):
    store.insert_records(conn, "s1", [_record("s1", "B:1", rh="r1")])
    store.insert_records(conn, "s1", [_record("s1", "B:1", rh="r2", ah="a9")])
    got = store.load_record_hashes(conn, "s1")
    assert len(got) == 1
    assert got["B:1"]["record_hash"] == "r2"
    assert got["B:1"]["arabic_hash"] == "a9"


def test_record_changes_roundtrip(conn):
    rows = [
        {"snapshot_id": "s1", "source_id": "B:1", "doc_status": "NEW",
         "changed_fields": None, "sentence_stats": None},
        {"snapshot_id": "s1", "source_id": "B:2", "doc_status": "MODIFIED",
         "changed_fields": ["arabic"],
         "sentence_stats": {"ar": {"added": 1, "removed": 0, "modified": 2},
                            "en": {"added": 0, "removed": 0, "modified": 0}}},
    ]
    store.record_changes(conn, "s1", rows)
    cur = conn.execute(
        "SELECT source_id, doc_status, changed_fields, sentence_stats FROM changes "
        "ORDER BY source_id")
    rows_out = cur.fetchall()
    assert rows_out[0] == ("B:1", "NEW", None, None)
    assert rows_out[1][1] == "MODIFIED"
    assert json.loads(rows_out[1][2]) == ["arabic"]
    assert json.loads(rows_out[1][3])["ar"]["modified"] == 2
    # idempotent re-record (same PK)
    store.record_changes(conn, "s1", [
        {"snapshot_id": "s1", "source_id": "B:1", "doc_status": "UNCHANGED",
         "changed_fields": None, "sentence_stats": None},
    ])
    assert conn.execute(
        "SELECT doc_status FROM changes WHERE snapshot_id='s1' AND source_id='B:1'"
    ).fetchone()[0] == "UNCHANGED"
    assert conn.execute("SELECT COUNT(*) FROM changes").fetchone()[0] == 2
