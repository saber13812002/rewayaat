"""Tests for corpus_sync.detect — doc/field/sentence change detection (T7).

Built on the REAL e2e fixture pair tests/fixtures/e2e_snapshots/
(Faḍaʾil al-Shīʿa, 45 docs from the local ES export; see
snap_A.meta.json / snap_B.meta.json for the exact, reproducible
mutations that derive B from A — no network needed).
"""

import json
from pathlib import Path

import pytest

from corpus_sync import detect, normalize, store

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "e2e_snapshots"
BOOK = "Faḍaʾil al-Shīʿa"
BOOK_SLUG = "fada-il-al-shi-a"
OLD_ID = "20261001-000000"
NEW_ID = "20261002-000000"
MODIFIED_ID = "Fadail-al-Shia-Saduq:2"
REMOVED_ID = "Fadail-al-Shia-Saduq:45"


def _load_lines(name: str) -> list[dict]:
    with open(FIXTURES / name, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _build_store(conn, a_lines, b_lines, *, new_status: str = "complete") -> None:
    store.record_snapshot(conn, snap_id=OLD_ID, created_at="2026-10-01T00:00:00Z",
                          status="complete", total_docs=len(a_lines),
                          raw_path=str(FIXTURES / "snap_A.jsonl"),
                          book_counts={BOOK: len(a_lines)})
    store.record_snapshot(conn, snap_id=NEW_ID, created_at="2026-10-02T00:00:00Z",
                          status=new_status, total_docs=len(b_lines),
                          raw_path=str(FIXTURES / "snap_B.jsonl"),
                          book_counts={BOOK: len(b_lines)})
    for snap, lines in ((OLD_ID, a_lines), (NEW_ID, b_lines)):
        rows = [normalize.normalize_doc(l["raw"], book_slug=BOOK_SLUG,
                                        fetched_at=l["fetched_at"]) for l in lines]
        store.insert_records(conn, snap, rows)


def test_change_categories_on_real_fixture_pair(tmp_path):
    conn = store.open_db(tmp_path / "db.sqlite")
    a_lines = _load_lines("snap_A.jsonl")
    b_lines = _load_lines("snap_B.jsonl")
    assert len(a_lines) == 45 and len(b_lines) == 44
    _build_store(conn, a_lines, b_lines)

    cs = detect.detect_changes(conn, old_snap_id=OLD_ID, new_snap_id=NEW_ID)

    assert cs.new == []
    assert cs.modified == [MODIFIED_ID]
    assert cs.removed == [REMOVED_ID]
    assert cs.unchanged == 43  # incl. :1/:10 re-fetched as-is (fetched_at differs)

    det = cs.details[MODIFIED_ID]
    assert det["changed_fields"] == ["english"]
    assert det["sentence_stats"]["en"]["modified"] >= 1
    assert det["sentence_stats"]["ar"]["modified"] == 0
    # only MODIFIED docs get details
    assert REMOVED_ID not in cs.details

    # changes table: 1 MODIFIED + 1 REMOVED row (UNCHANGED not stored)
    rows = conn.execute(
        "SELECT source_id, doc_status FROM changes"
        " WHERE snapshot_id=? ORDER BY source_id", (NEW_ID,)).fetchall()
    assert len(rows) == 2
    assert {r[1] for r in rows} == {"MODIFIED", "REMOVED"}


def test_partial_snapshot_blocks_removed(tmp_path):
    conn = store.open_db(tmp_path / "db.sqlite")
    a_lines = _load_lines("snap_A.jsonl")
    b_lines = _load_lines("snap_B.jsonl")
    _build_store(conn, a_lines, b_lines, new_status="partial")

    with pytest.raises(detect.IncompleteSnapshotError):
        detect.detect_changes(conn, old_snap_id=OLD_ID, new_snap_id=NEW_ID)
    # nothing written before the check
    n = conn.execute("SELECT COUNT(*) FROM changes").fetchone()[0]
    assert n == 0


def test_new_docs_detected(tmp_path):
    """Synthetic pair: 1 shared doc + 1 doc only in the new snapshot."""
    conn = store.open_db(tmp_path / "db.sqlite")
    shared = {"_id": "Bk:1", "book": "Bk", "number": "1", "chapter": "C",
              "arabic": "أ.", "english": "One."}
    added = {"_id": "Bk:2", "book": "Bk", "number": "2", "chapter": "C",
             "arabic": "ب.", "english": "Two."}

    def write(path: Path, docs, fetched: str) -> None:
        with open(path, "w", encoding="utf-8") as fh:
            for d in docs:
                fh.write(json.dumps({"source_id": d["_id"], "book": "Bk",
                                     "fetched_at": fetched, "page": 1,
                                     "label": "book", "raw": d},
                                    ensure_ascii=False) + "\n")

    pa, pb = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    write(pa, [shared], "2026-10-01T00:00:00Z")
    write(pb, [shared, added], "2026-10-02T00:00:00Z")
    store.record_snapshot(conn, snap_id=OLD_ID, created_at="2026-10-01T00:00:00Z",
                          status="complete", total_docs=1, raw_path=str(pa),
                          book_counts={"Bk": 1})
    store.record_snapshot(conn, snap_id=NEW_ID, created_at="2026-10-02T00:00:00Z",
                          status="complete", total_docs=2, raw_path=str(pb),
                          book_counts={"Bk": 2})
    store.insert_records(
        conn, OLD_ID,
        [normalize.normalize_doc(shared, book_slug="bk",
                                 fetched_at="2026-10-01T00:00:00Z")])
    store.insert_records(
        conn, NEW_ID,
        [normalize.normalize_doc(d, book_slug="bk",
                                 fetched_at="2026-10-02T00:00:00Z")
         for d in (shared, added)])

    cs = detect.detect_changes(conn, old_snap_id=OLD_ID, new_snap_id=NEW_ID)
    assert cs.new == ["Bk:2"]
    assert cs.modified == []
    assert cs.removed == []
    assert cs.unchanged == 1
