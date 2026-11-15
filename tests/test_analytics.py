"""Tests for corpus_sync.analytics — monthly metrics + report rendering (T8).

Synthetic store with controlled snapshots/hashes (raw JSONL generated in
tmp_path so detect_changes and the sentence-total cache run on real
files). Formulas pinned here are the spec formulas (plan Task 8):

- docs_*      counts from the `changes` rows detect_changes wrote for the
              new snapshot (NEW/MODIFIED/REMOVED); unchanged = total - new - modified
- sentences_new_*    split_sentences count of each NEW doc's raw text (new snapshot)
- sentences_modified_*  sum of sentence_stats[*].modified over MODIFIED rows
- sentences_removed_*   split_sentences count of each REMOVED doc's raw text (old snapshot)
- pct_docs_growth   = docs_new / old_total * 100   (0.0 if no old snapshot)
- pct_docs_removed  = docs_removed / old_total * 100
- pct_text_change_* = (sentences_new_* + sentences_modified_*)
                      / current snapshot total sentences of lang * 100
- vs_previous_month   metrics re-based on the previous complete snapshot,
                      nested back through the complete-snapshot chain
- vs_baseline     record_hash diff from the FIRST complete snapshot to the
                  new snapshot (cumulative)
- history         one row per complete snapshot (its month, its changes-row
                  counts or 0, its docs_total)
"""

import json
from pathlib import Path

import pytest

from corpus_sync import analytics, detect, store

BOOKS = ("B1", "B2", "B3")


def _raw_doc(num: int, book: str) -> dict:
    return {
        "_id": f"Book:{num}",
        "book": book,
        "number": str(num),
        "chapter": f"Chapter {num}",
        "arabic": f"جملة عربي رقم {num} .",
        "english": f"English sentence number {num}.",
    }


def _write_raw(path: Path, docs: list[dict], fetched: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for d in docs:
            fh.write(json.dumps(
                {"source_id": d["_id"], "book": d["book"],
                 "fetched_at": fetched, "page": 1, "label": "book",
                 "raw": d}, ensure_ascii=False) + "\n")


def _install(conn, snap_id: str, created: str, raw: Path, fetched: str,
             docs: list[dict]) -> None:
    _write_raw(raw, docs, fetched)
    store.record_snapshot(conn, snap_id=snap_id, created_at=created,
                          status="complete", total_docs=len(docs),
                          raw_path=str(raw),
                          book_counts={b: sum(1 for d in docs if d["book"] == b)
                                       for b in BOOKS})
    store.insert_records(conn, snap_id,
                         [normalize_row(d, fetched) for d in docs])


def normalize_row(d: dict, fetched: str) -> dict:
    from corpus_sync import normalize

    return normalize.normalize_doc(d, book_slug=d["book"].lower(),
                                   fetched_at=fetched)


def _docs_100() -> list[dict]:
    docs = [_raw_doc(n, "B1") for n in range(1, 31)]
    docs += [_raw_doc(n, "B2") for n in range(31, 81)]
    docs += [_raw_doc(n, "B3") for n in range(81, 101)]
    return docs


def _docs_next(docs100: list[dict]) -> list[dict]:
    """100 - 1 removed + 2 new = 101 docs; 2 modified in place.

    Modifications are in-sentence word insertions so the diff's containment
    pairing counts them as 'modified' (a bare append after the final period
    would count as 'added'). The Book:22 arabic edit also adds one sentence,
    so the current snapshot's ar total grows to 102 (see the 3/102 assert).
    """
    docs = [dict(d) for d in docs100]
    docs = [d for d in docs if d["_id"] != "Book:41"]
    for d in docs:
        if d["_id"] == "Book:22":
            d["arabic"] = d["arabic"][:-2] + " مضاف. زيادة."
        if d["_id"] == "Book:42":
            d["english"] = d["english"][:-1] + " Extra."
    docs.append({"_id": "Book:104", "book": "B1", "number": "104",
                 "chapter": "Chapter 104", "arabic": "جديد .",
                 "english": "New one."})
    docs.append({"_id": "Book:105", "book": "B3", "number": "105",
                 "chapter": "Chapter 105", "arabic": "جديد .",
                 "english": "New two."})
    return docs


def _metrics_for(conn, tmp_path: Path, month: str, new_id: str, old_id):
    return analytics.monthly_metrics(conn, month=month, new_snap_id=new_id,
                                     old_snap_id=old_id)


def test_monthly_metrics_formulas_exact(tmp_path):
    conn = store.open_db(tmp_path / "db.sqlite")
    d100 = _docs_100()
    _install(conn, "20260101-000000", "2026-01-01T00:00:00Z",
             tmp_path / "m1.jsonl", "2026-01-01T00:00:00Z", d100)
    d101 = _docs_next(d100)
    _install(conn, "20260201-000000", "2026-02-01T00:00:00Z",
             tmp_path / "m2.jsonl", "2026-02-01T00:00:00Z", d101)
    detect.detect_changes(conn, old_snap_id="20260101-000000",
                          new_snap_id="20260201-000000")

    m = _metrics_for(conn, tmp_path, "2026-02",
                     "20260201-000000", "20260101-000000")

    assert set(m) == {
        "month", "new_snapshot", "old_snapshot", "docs_total",
        "docs_new", "docs_modified", "docs_removed", "docs_unchanged",
        "sentences_new_ar", "sentences_modified_ar", "sentences_removed_ar",
        "sentences_new_en", "sentences_modified_en", "sentences_removed_en",
        "pct_docs_growth", "pct_text_change_ar", "pct_text_change_en",
        "pct_docs_removed", "per_book", "vs_previous_month",
        "vs_baseline", "history"}
    assert m["month"] == "2026-02"
    assert m["new_snapshot"] == "20260201-000000"
    assert m["old_snapshot"] == "20260101-000000"
    assert m["docs_total"] == 101
    assert m["docs_new"] == 2
    assert m["docs_modified"] == 2
    assert m["docs_removed"] == 1
    assert m["docs_unchanged"] == 97
    # sentence stats from the real sentence diff / raw texts
    assert m["sentences_new_ar"] == 2
    assert m["sentences_modified_ar"] == 1
    assert m["sentences_removed_ar"] == 1
    assert m["sentences_new_en"] == 2
    assert m["sentences_modified_en"] == 1
    assert m["sentences_removed_en"] == 1
    # current snapshot totals: ar 102 (Book:22 gained a sentence), en 101
    assert m["pct_docs_growth"] == 2 / 100 * 100
    assert m["pct_text_change_ar"] == pytest.approx(3 / 102 * 100)
    assert m["pct_text_change_en"] == pytest.approx(3 / 101 * 100)
    assert m["pct_docs_removed"] == 1 / 100 * 100
    assert m["per_book"] == {
        "B1": {"new": 1, "modified": 1, "removed": 0},
        "B2": {"new": 0, "modified": 1, "removed": 1},
        "B3": {"new": 1, "modified": 0, "removed": 0}}
    # previous complete snapshot (Jan) has no earlier one of its own
    assert m["vs_previous_month"] is not None
    assert m["vs_previous_month"]["vs_previous_month"] is None
    # baseline is the first complete snapshot (Jan): cumulative diff = same
    assert m["vs_baseline"]["docs_new"] == 2
    assert m["vs_baseline"]["docs_modified"] == 2
    assert m["vs_baseline"]["docs_removed"] == 1
    assert m["vs_baseline"]["baseline_snapshot"] == "20260101-000000"
    assert m["history"] == [
        {"month": "2026-01", "docs_new": 0, "docs_modified": 0,
         "docs_removed": 0, "docs_total": 100},
        {"month": "2026-02", "docs_new": 2, "docs_modified": 2,
         "docs_removed": 1, "docs_total": 101}]


def test_sentence_totals_cached_once(tmp_path):
    conn = store.open_db(tmp_path / "db.sqlite")
    d100 = _docs_100()
    raw = tmp_path / "m.jsonl"
    _write_raw(raw, d100, "2026-01-01T00:00:00Z")
    store.record_snapshot(conn, snap_id="20260101-000000",
                          created_at="2026-01-01T00:00:00Z", status="complete",
                          total_docs=100, raw_path=str(raw),
                          book_counts={b: sum(1 for d in d100
                                              if d["book"] == b)
                                       for b in BOOKS})
    analytics.ensure_sentence_totals(conn, "20260101-000000", raw)
    analytics.ensure_sentence_totals(conn, "20260101-000000", raw)  # idempotent
    rows = dict(conn.execute(
        "SELECT lang, total FROM sentence_totals"
        " WHERE snapshot_id='20260101-000000'").fetchall())
    assert rows == {"en": 100, "ar": 100}


def test_vs_previous_month_and_report(tmp_path):
    conn = store.open_db(tmp_path / "db.sqlite")
    d100 = _docs_100()
    _install(conn, "20260901-000000", "2026-09-01T00:00:00Z",
             tmp_path / "m9.jsonl", "2026-09-01T00:00:00Z", d100)
    docs_sep_next = [dict(d) for d in d100]
    for d in docs_sep_next:
        if d["_id"] == "Book:5":
            d["english"] = d["english"][:-1] + " Added."
    _install(conn, "20261015-000000", "2026-10-15T00:00:00Z",
             tmp_path / "m10.jsonl", "2026-10-15T00:00:00Z", docs_sep_next)
    detect.detect_changes(conn, old_snap_id="20260901-000000",
                          new_snap_id="20261015-000000")

    m = _metrics_for(conn, tmp_path, "2026-10",
                     "20261015-000000", "20260901-000000")
    assert m["docs_modified"] == 1
    assert m["sentences_modified_en"] == 1
    assert m["sentences_modified_ar"] == 0

    pm = m["vs_previous_month"]
    # previous complete snapshot is Sep; re-based on it, same Oct diff
    assert pm is not None
    assert pm["old_snapshot"] == "20260901-000000"
    assert pm["docs_modified"] == 1
    assert pm["docs_new"] == 0
    assert pm["docs_removed"] == 0
    # no complete snapshot exists before Sep
    assert pm["vs_previous_month"] is None
    assert m["history"] == [
        {"month": "2026-09", "docs_new": 0, "docs_modified": 0,
         "docs_removed": 0, "docs_total": 100},
        {"month": "2026-10", "docs_new": 0, "docs_modified": 1,
         "docs_removed": 0, "docs_total": 100}]

    report = analytics.render_report("2026-10", m, baseline_month="2026-09")
    assert "# Rewayaat corpus change report — 2026-10" in report
    assert "2026-09" in report
    assert "20261015-000000" in report
    assert f"{m['pct_docs_growth']:.2f}%" in report
    assert f"{m['pct_text_change_en']:.2f}%" in report
    assert f"{m['pct_docs_removed']:.2f}%" in report
    for value in (m["docs_total"], m["docs_modified"], m["docs_unchanged"]):
        assert str(value) in report
    for book in BOOKS:
        assert book in report
    assert "vs baseline" in report.lower()
    assert m["vs_baseline"]["baseline_snapshot"] in report


def test_first_month_no_previous(tmp_path):
    conn = store.open_db(tmp_path / "db.sqlite")
    d100 = _docs_100()
    _install(conn, "20260101-000000", "2026-01-01T00:00:00Z",
             tmp_path / "m1.jsonl", "2026-01-01T00:00:00Z", d100)
    m = _metrics_for(conn, tmp_path, "2026-01",
                     "20260101-000000", None)
    assert m["old_snapshot"] is None
    assert m["vs_previous_month"] is None
    assert m["pct_docs_growth"] == 0.0
    assert m["pct_docs_removed"] == 0.0
    assert m["docs_new"] == 0
    assert m["vs_baseline"]["docs_new"] == 0
    assert m["vs_baseline"]["baseline_snapshot"] == "20260101-000000"
