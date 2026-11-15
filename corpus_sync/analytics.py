# -*- coding: utf-8 -*-
"""Monthly corpus analytics: spec formulas + markdown report (T8).

Metrics are computed for a (new, old) snapshot pair from the rows
``detect_changes`` persisted in the ``changes`` table, the raw JSONL
snapshots, and the normalized record hashes. Formulas are pinned by
tests/test_analytics.py:

- docs_*              from the new snapshot's changes rows;
                      docs_unchanged = docs_total - new - modified
- sentences_new_*         split_sentences count of NEW docs' raw text
                          (new snapshot's raw file)
- sentences_modified_*    sum of sentence_stats[*].modified over MODIFIED rows
- sentences_removed_*     split_sentences count of REMOVED docs' raw text
                          (old snapshot's raw file)
- pct_docs_growth         docs_new / old_total * 100    (0.0 if no old snapshot)
- pct_docs_removed        docs_removed / old_total * 100
- pct_text_change_*       (sentences_new_* + sentences_modified_*)
                          / current snapshot's total sentences of that lang * 100
  (the denominator is cached in the ``sentence_totals`` table via
  ``ensure_sentence_totals``)
- per_book            union of both snapshots' book_counts; new/modified
                      attributed via the new snapshot's raw lines, removed via
                      the old snapshot's raw lines
- vs_previous_month   the same metric dict re-based on each earlier complete
                      snapshot, nested back through the complete-snapshot
                      chain (None if there is none)
- vs_baseline         record_hash diff from the FIRST complete snapshot to
                      the new snapshot (cumulative)
- history             one row per complete snapshot: its month, its
                      changes-row counts (0 if none), its docs_total
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from . import normalize, store

_LANGS = (("ar", "arabic"), ("en", "english"))


def _month_of(snap_id: str) -> str:
    return f"{snap_id[:4]}-{snap_id[4:6]}"


def _snap_meta(conn: sqlite3.Connection, snap_id: str):
    """(total_docs, raw_path, book_counts) for a snapshot, or None."""
    row = conn.execute(
        "SELECT total_docs, raw_path, book_counts FROM snapshots WHERE id=?",
        (snap_id,)).fetchone()
    if row is None:
        return None
    return row[0], row[1], json.loads(row[2])


def _raw_index(raw_path: Path) -> dict[str, dict]:
    """source_id -> {"raw": raw doc, "book": line-level book name}."""
    out: dict[str, dict] = {}
    with open(raw_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            out[rec["source_id"]] = {"raw": rec["raw"],
                                     "book": rec.get("book", "")}
    return out


def _sentences_of(raw_doc: dict) -> dict[str, int]:
    return {lang: len(normalize.split_sentences(
        (raw_doc or {}).get(key) or "", lang))
        for lang, key in _LANGS}


def ensure_sentence_totals(conn: sqlite3.Connection, snap_id: str,
                           raw_path: Path) -> None:
    """Cache the snapshot's raw-text sentence totals (idempotent)."""
    existing = {lang for (lang,) in conn.execute(
        "SELECT lang FROM sentence_totals WHERE snapshot_id=?", (snap_id,))}
    if {"ar", "en"} <= existing:
        return
    totals = {"ar": 0, "en": 0}
    with open(raw_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)["raw"]
            for lang, key in _LANGS:
                totals[lang] += len(normalize.split_sentences(
                    raw.get(key) or "", lang))
    conn.executemany(
        "INSERT OR REPLACE INTO sentence_totals(snapshot_id, lang, total)"
        " VALUES(?,?,?)",
        [(snap_id, lang, totals[lang]) for lang in ("ar", "en")])
    conn.commit()


def _sentence_totals(conn: sqlite3.Connection, snap_id: str) -> dict[str, int]:
    return {lang: total for lang, total in conn.execute(
        "SELECT lang, total FROM sentence_totals WHERE snapshot_id=?",
        (snap_id,))}


def _prev_complete(conn: sqlite3.Connection, snap_id: str) -> str | None:
    """Most recent complete snapshot strictly older than ``snap_id``."""
    row = conn.execute(
        "SELECT id FROM snapshots WHERE status='complete' AND id<?"
        " ORDER BY id DESC LIMIT 1", (snap_id,)).fetchone()
    return row[0] if row else None


def _vs_baseline(conn: sqlite3.Connection, new_snap_id: str,
                 docs_total: int) -> dict:
    """Cumulative record_hash diff: first complete snapshot vs the new one."""
    row = conn.execute(
        "SELECT id FROM snapshots WHERE status='complete'"
        " ORDER BY id ASC LIMIT 1").fetchone()
    base_id = row[0] if row else None
    out = {"baseline_snapshot": base_id, "docs_total": docs_total,
           "docs_new": 0, "docs_modified": 0, "docs_removed": 0,
           "docs_unchanged": 0}
    if base_id is None or base_id == new_snap_id:
        return out
    base = store.load_record_hashes(conn, base_id)
    new = store.load_record_hashes(conn, new_snap_id)
    common = set(base) & set(new)
    out["docs_new"] = len(set(new) - set(base))
    out["docs_removed"] = len(set(base) - set(new))
    out["docs_modified"] = sum(
        1 for s in common if base[s]["record_hash"] != new[s]["record_hash"])
    out["docs_unchanged"] = len(common) - out["docs_modified"]
    return out


def _history(conn: sqlite3.Connection) -> list[dict]:
    """One row per complete snapshot (chronological): month, change counts, total."""
    history: list[dict] = []
    for snap_id, total in conn.execute(
            "SELECT id, total_docs FROM snapshots WHERE status='complete'"
            " ORDER BY id ASC"):
        cnt = {"NEW": 0, "MODIFIED": 0, "REMOVED": 0}
        for status, n in conn.execute(
                "SELECT doc_status, COUNT(*) FROM changes WHERE snapshot_id=?"
                " GROUP BY doc_status", (snap_id,)):
            if status in cnt:
                cnt[status] = n
        history.append({"month": _month_of(snap_id),
                        "docs_new": cnt["NEW"],
                        "docs_modified": cnt["MODIFIED"],
                        "docs_removed": cnt["REMOVED"],
                        "docs_total": total})
    return history


def _core_metrics(conn: sqlite3.Connection, month: str, new_snap_id: str,
                  old_snap_id: str | None) -> dict:
    """The 20 measured metric values for a (new, old) pair.

    ``vs_previous_month`` is attached by :func:`monthly_metrics` (chain).
    """
    new_meta = _snap_meta(conn, new_snap_id)
    if new_meta is None:
        raise ValueError(f"unknown snapshot: {new_snap_id!r}")
    docs_total, new_raw_path, new_books = new_meta
    new_raw = _raw_index(store.resolve_raw_path(new_raw_path))

    old_meta = _snap_meta(conn, old_snap_id) if old_snap_id else None
    old_total = old_meta[0] if old_meta else 0
    old_raw = _raw_index(store.resolve_raw_path(old_meta[1])) if old_meta else {}
    old_books = old_meta[2] if old_meta else {}

    new_sids: list[str] = []
    mod_sids: list[str] = []
    rem_sids: list[str] = []
    mod_stats: dict[str, dict] = {}
    for sid, status, stats_json in conn.execute(
            "SELECT source_id, doc_status, sentence_stats FROM changes"
            " WHERE snapshot_id=?", (new_snap_id,)):
        if status == "NEW":
            new_sids.append(sid)
        elif status == "MODIFIED":
            mod_sids.append(sid)
            if stats_json:
                mod_stats[sid] = json.loads(stats_json)
        elif status == "REMOVED":
            rem_sids.append(sid)

    sent = {lang: {"new": 0, "modified": 0, "removed": 0}
            for lang in ("ar", "en")}
    for sid in new_sids:
        for lang, n in _sentences_of(new_raw.get(sid, {}).get("raw")).items():
            sent[lang]["new"] += n
    for sid in mod_sids:
        stats = mod_stats.get(sid) or {}
        for lang in ("ar", "en"):
            sent[lang]["modified"] += (stats.get(lang) or {}).get("modified", 0)
    for sid in rem_sids:
        for lang, n in _sentences_of(old_raw.get(sid, {}).get("raw")).items():
            sent[lang]["removed"] += n

    ensure_sentence_totals(conn, new_snap_id, store.resolve_raw_path(new_raw_path))
    totals = _sentence_totals(conn, new_snap_id)

    def _pct(part: int, whole: int) -> float:
        return part / whole * 100 if whole else 0.0

    per_book: dict[str, dict] = {
        b: {"new": 0, "modified": 0, "removed": 0}
        for b in sorted(set(new_books) | set(old_books))}
    for sid in new_sids:
        per_book.setdefault(new_raw.get(sid, {}).get("book", ""),
                            {"new": 0, "modified": 0, "removed": 0})["new"] += 1
    for sid in mod_sids:
        per_book.setdefault(new_raw.get(sid, {}).get("book", ""),
                            {"new": 0, "modified": 0, "removed": 0})["modified"] += 1
    for sid in rem_sids:
        per_book.setdefault(old_raw.get(sid, {}).get("book", ""),
                            {"new": 0, "modified": 0, "removed": 0})["removed"] += 1

    return {
        "month": month,
        "new_snapshot": new_snap_id,
        "old_snapshot": old_snap_id,
        "docs_total": docs_total,
        "docs_new": len(new_sids),
        "docs_modified": len(mod_sids),
        "docs_removed": len(rem_sids),
        "docs_unchanged": docs_total - len(new_sids) - len(mod_sids),
        "sentences_new_ar": sent["ar"]["new"],
        "sentences_modified_ar": sent["ar"]["modified"],
        "sentences_removed_ar": sent["ar"]["removed"],
        "sentences_new_en": sent["en"]["new"],
        "sentences_modified_en": sent["en"]["modified"],
        "sentences_removed_en": sent["en"]["removed"],
        "pct_docs_growth": _pct(len(new_sids), old_total),
        "pct_text_change_ar": _pct(sent["ar"]["new"] + sent["ar"]["modified"],
                                   totals.get("ar", 0)),
        "pct_text_change_en": _pct(sent["en"]["new"] + sent["en"]["modified"],
                                   totals.get("en", 0)),
        "pct_docs_removed": _pct(len(rem_sids), old_total),
        "per_book": per_book,
        "vs_baseline": _vs_baseline(conn, new_snap_id, docs_total),
        "history": _history(conn),
    }


def monthly_metrics(conn: sqlite3.Connection, *, month: str,
                    new_snap_id: str, old_snap_id: str | None) -> dict:
    """Full 21-key metric dict for the new snapshot (spec formulas).

    ``vs_previous_month`` is a chain: the first link re-bases the same core
    metrics on the latest complete snapshot older than ``new_snap_id``; each
    subsequent link re-bases on the complete snapshot before that one, until
    the chain is exhausted (final link is None).
    """
    m = _core_metrics(conn, month, new_snap_id, old_snap_id)
    nodes: list[dict] = []
    prev = _prev_complete(conn, new_snap_id)
    while prev is not None:
        nodes.append(_core_metrics(conn, _month_of(prev), new_snap_id, prev))
        prev = _prev_complete(conn, prev)
    for i in range(len(nodes) - 1):
        nodes[i]["vs_previous_month"] = nodes[i + 1]
    if nodes:
        nodes[-1]["vs_previous_month"] = None
        m["vs_previous_month"] = nodes[0]
    else:
        m["vs_previous_month"] = None
    return m


def render_report(month: str, m: dict, *, baseline_month: str) -> str:
    """Markdown change report: every measured value, nothing else."""
    lines = [
        f"# Rewayaat corpus change report — {month}",
        "",
        f"- Month: {month}",
        f"- Baseline month: {baseline_month}",
        f"- New snapshot: {m['new_snapshot']}",
        f"- Old snapshot: {m['old_snapshot'] or '- (first snapshot)'}",
        "",
        "## Document counts",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| docs_total | {m['docs_total']} |",
        f"| docs_new | {m['docs_new']} |",
        f"| docs_modified | {m['docs_modified']} |",
        f"| docs_removed | {m['docs_removed']} |",
        f"| docs_unchanged | {m['docs_unchanged']} |",
        f"| pct_docs_growth | {m['pct_docs_growth']:.2f}% |",
        f"| pct_docs_removed | {m['pct_docs_removed']:.2f}% |",
        "",
        "## Sentence changes",
        "",
        "| Metric | Arabic | English |",
        "|---|---|---|",
        f"| new | {m['sentences_new_ar']} | {m['sentences_new_en']} |",
        f"| modified | {m['sentences_modified_ar']} | {m['sentences_modified_en']} |",
        f"| removed | {m['sentences_removed_ar']} | {m['sentences_removed_en']} |",
        f"| pct_text_change | {m['pct_text_change_ar']:.2f}% | "
        f"{m['pct_text_change_en']:.2f}% |",
        "",
        "## Per book",
        "",
        "| Book | new | modified | removed |",
        "|---|---|---|---|",
    ]
    for book in sorted(m["per_book"]):
        pb = m["per_book"][book]
        lines.append(f"| {book} | {pb['new']} | {pb['modified']} "
                     f"| {pb['removed']} |")
    vb = m["vs_baseline"]
    lines += [
        "",
        "## vs baseline",
        "",
        f"Baseline snapshot: {vb['baseline_snapshot']}",
        "",
        f"- docs_new: {vb['docs_new']}",
        f"- docs_modified: {vb['docs_modified']}",
        f"- docs_removed: {vb['docs_removed']}",
        f"- docs_unchanged: {vb['docs_unchanged']}",
        "",
        "## History",
        "",
        "| Month | new | modified | removed | total |",
        "|---|---|---|---|---|",
    ]
    for h in m["history"]:
        lines.append(f"| {h['month']} | {h['docs_new']} | {h['docs_modified']}"
                     f" | {h['docs_removed']} | {h['docs_total']} |")
    return "\n".join(lines) + "\n"
