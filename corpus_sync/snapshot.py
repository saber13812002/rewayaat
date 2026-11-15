# -*- coding: utf-8 -*-
"""Snapshot orchestrator: full-corpus extraction from the rewayaat.info API.

One snapshot = one JSONL file in RAW_DIR, one line per narration:
    {"source_id", "book", "fetched_at", "page", "label", "raw"}

Strategy (locked by live smoke, see tests/fixtures/smoke_range_note.md):
- LOOKUP totals clamp at max_result_window=10,000, so books with live count
  >= MAX_PAGE_SIZE are split into one query per chapter (chapter list from the
  committed tests/fixtures/chapter_split.json fixture — live facets cap at 500
  buckets). Smaller books use a single book-scope query.
- Chapter scope values must be quote-free (server parseFieldScopes regex);
  double quotes in chapter names are replaced by '*' wildcards (verified live:
  exact totals for even and odd quote counts).
- Wildcard over-match (substring sibling chapters) is harmless: final output
  is deduped by source_id (keep last) and gated by per-book counts.
- Completeness: per-book unique-doc count vs /v1/browse/books live count.
- Shortfall repair for split books: number:"<n>" exact-term queries (number is
  a simple-value field -> term query on the server) over the number seed set
  from the split fixture union numbers seen in the file, descending, with
  early stop once the book reaches its live count.

Resumability: after every page, STATE_DIR/snapshot_state.json is rewritten
atomically with the completed (label, page) pairs and per-label totals. A run
with an existing dest resumes from that state and skips completed pages.

The state file's sha256 field carries the hash of the last finalized file
(recomputed at finalize; per-page hashing of a growing 50MB file was dropped
for performance — deliberate deviation from the plan's per-page sha256).
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import config

# LOOKUP max_result_window: totals clamp here; from >= this returns size=0.
MAX_PAGE_SIZE: int = 10000

STATE_FILE = "snapshot_state.json"

_seq = itertools.count(1)


class IncompleteSnapshotError(RuntimeError):
    """Raised in strict mode when at least one book is incomplete."""

    def __init__(self, incomplete: list[str]):
        super().__init__(f"incomplete books: {', '.join(incomplete)}")
        self.incomplete = list(incomplete)


@dataclass(frozen=True)
class LookupResult:
    """One LOOKUP page. Mirrors corpus_sync.fetch.LookupResult (re-exported so
    fakes/tests depend only on this module)."""

    total: int
    collection: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class Query:
    book: str
    chapter: str | None      # exact chapter title (label + repair bookkeeping)
    label: str               # "book:X" | "book:X chapter:<name>" | "book:X number:<n>"
    expected_count: int      # book-kind: live count at plan time; else 0
    kind: str                # "book" | "chapter" | "number"
    number: str | None = None  # kind == "number" only


@dataclass
class SnapshotResult:
    path: Path
    docs: int                # unique doc count (after dedupe)
    books: dict[str, int]    # per-book unique doc counts
    incomplete: list[str]    # book names not matching live counts
    duration_s: float


def query_for(q: Query) -> str:
    """Server query string for a Query (field-scope syntax, double-quoted)."""
    if q.kind == "number":
        return f'book:"{q.book}" number:"{q.number}"'
    if q.kind == "chapter":
        safe = q.chapter.replace('"', "*")  # keep scope value quote-free
        return f'book:"{q.book}" chapter:"{safe}"'
    return f'book:"{q.book}"'


def plan_queries(books: list, split: dict | None) -> list[Query]:
    """One query per book, or one per chapter for >= MAX_PAGE_SIZE books.

    `split` is the parsed chapter_split.json fixture; books absent from it
    (or with no chapter list) fall back to a single book-scope query.
    """
    split_books = (split or {}).get("books", {})
    qs: list[Query] = []
    for b in books:
        entry = split_books.get(b.name)
        chapters = (entry or {}).get("chapters") or []
        if b.count >= MAX_PAGE_SIZE and chapters:
            for c in chapters:
                name = c["name"]
                qs.append(
                    Query(
                        book=b.name,
                        chapter=name,
                        label=f"book:{b.name} chapter:{name}",
                        expected_count=0,
                        kind="chapter",
                    )
                )
        else:
            qs.append(
                Query(
                    book=b.name,
                    chapter=None,
                    label=f"book:{b.name}",
                    expected_count=b.count,
                    kind="book",
                )
            )
    return qs


def _default_clock() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _default_dest() -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return config.RAW_DIR / f"snapshot-{ts}-{next(_seq)}.jsonl"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _load_state(state_path: Path, dest_name: str) -> dict | None:
    if not state_path.is_file():
        return None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if state.get("snapshot") != dest_name:
        return None
    state.setdefault("completed", {})
    state.setdefault("totals", {})
    state.setdefault("repair_tried", {})
    state.setdefault("sha256", "")
    return state


def _save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(state_path, json.dumps(state, ensure_ascii=False, indent=1))


def _read_lines(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _dedupe(lines: list[dict]) -> list[dict]:
    """Dedupe by source_id, keep last occurrence; deterministic (book, number) order."""

    def sort_key(l):
        raw = l.get("raw") or {}
        num = str(raw.get("number", ""))
        return (l.get("book", ""), (0, (len(num), num)) if num.isdigit() else (1, num))

    last: dict[str, dict] = {}
    order: list[str] = []
    for l in lines:
        sid = l["source_id"]
        if sid not in last:
            order.append(sid)
        last[sid] = l
    docs = [last[sid] for sid in order]
    docs.sort(key=sort_key)
    return docs


def verify_completeness(book_counts: dict[str, int], live_books: list) -> list[str]:
    """Book names whose unique snapshot count differs from the live catalog.

    Missing (0) and extra (not in live catalog) books are both reported.
    """
    live = {b.name: b.count for b in live_books}
    bad = []
    for name in sorted(set(book_counts) | set(live)):
        if book_counts.get(name, 0) != live.get(name, 0):
            bad.append(name)
    return bad


def run_snapshot(
    *,
    dest: Path | None = None,
    fetcher=None,
    clock: "callable | None" = None,
    per_page: int = config.PER_PAGE,
    strict: bool = False,
    split: dict | None = None,
) -> SnapshotResult:
    """Fetch the full corpus into one JSONL snapshot.

    `fetcher` must expose books(), facets(name), lookup(q, page, per_page)
    (corpus_sync.fetch satisfies this). `dest` selects the snapshot file;
    None -> a fresh timestamped file under RAW_DIR. An existing dest with a
    matching state file resumes (completed pages are skipped).
    """
    started = time.monotonic()
    from . import fetch as fetch_module

    fetcher = fetcher or fetch_module
    clock = clock or _default_clock
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)

    dest = dest or _default_dest()
    dest.parent.mkdir(parents=True, exist_ok=True)
    state_path = config.STATE_DIR / STATE_FILE
    state = _load_state(state_path, dest.name)
    is_resume = state is not None
    if not is_resume:
        state = {"snapshot": dest.name, "completed": {}, "totals": {}, "repair_tried": {}, "sha256": ""}
        _save_state(state_path, state)

    live = {b.name: b for b in fetcher.books()}
    queries = plan_queries(list(live.values()), split)

    # --- planned passes (book / chapter queries) --------------------------
    for q in queries:
        done = set(state["completed"].get(q.label, []))
        total = state["totals"].get(q.label)
        first_docs = None
        if total is None:
            first = fetcher.lookup(query_for(q), 1, per_page)
            total = first.total
            first_docs = first.collection
            state["totals"][q.label] = total
            _save_state(state_path, state)
        pages = (total + per_page - 1) // per_page if total else 0
        for page in range(1, pages + 1):
            if page in done:
                continue
            if page == 1 and first_docs is not None:
                res = LookupResult(total=total, collection=first_docs)
            else:
                res = fetcher.lookup(query_for(q), page, per_page)
            _append_page(dest, res.collection, q.book, clock(), page, q.label)
            done.add(page)
            state["completed"][q.label] = sorted(done)
            _save_state(state_path, state)

    # --- dedupe + book counts ---------------------------------------------
    lines = _dedupe(_read_lines(dest))
    book_counts: dict[str, int] = {}
    for l in lines:
        book_counts[l["book"]] = book_counts.get(l["book"], 0) + 1

    # --- repair pass: number-term scan for split books with shortfalls ----
    split_books = (split or {}).get("books", {})
    for name, live_b in live.items():
        if live_b.count < MAX_PAGE_SIZE:
            continue
        if name not in split_books or not book_counts.get(name, 0) < live_b.count:
            continue
        entry = split_books[name]
        seed = [str(n) for n in entry.get("numbers", [])]
        seen_ids = {l["source_id"] for l in lines if l["book"] == name}
        seen = {
            str((l.get("raw") or {}).get("number"))
            for l in lines
            if l["book"] == name and (l.get("raw") or {}).get("number") is not None
        }
        numbers = sorted(set(seed) | seen, key=_num_key, reverse=True)
        tried = set(state["repair_tried"].get(name, []))
        count = book_counts.get(name, 0)
        for n in numbers:
            if count >= live_b.count:
                break
            if n in tried or n == "":
                continue
            tried.add(n)
            rq = Query(book=name, chapter=None, label=f"book:{name} number:{n}", expected_count=0, kind="number", number=n)
            first = fetcher.lookup(query_for(rq), 1, per_page)
            count += _count_new(first.collection, seen_ids)
            _append_page(dest, first.collection, name, clock(), 1, rq.label)
            pages = (first.total + per_page - 1) // per_page if first.total else 0
            for page in range(2, pages + 1):
                res = fetcher.lookup(query_for(rq), page, per_page)
                count += _count_new(res.collection, seen_ids)
                _append_page(dest, res.collection, name, clock(), page, rq.label)
            state["repair_tried"][name] = sorted(tried)
            _save_state(state_path, state)

    # --- finalize: dedupe rewrite + completeness ---------------------------
    lines = _dedupe(_read_lines(dest))
    book_counts = {}
    for l in lines:
        book_counts[l["book"]] = book_counts.get(l["book"], 0) + 1
    _atomic_write(
        dest, "".join(json.dumps(l, ensure_ascii=False) + "\n" for l in lines)
    )
    sha = _sha256(dest)
    state["sha256"] = sha
    _save_state(state_path, state)

    incomplete = verify_completeness(book_counts, list(live.values()))
    summary = {
        "snapshot": dest.name,
        "docs": len(lines),
        "books": book_counts,
        "incomplete": incomplete,
        "sha256": sha,
        "fetched_at": clock(),
        "duration_s": round(time.monotonic() - started, 2),
    }
    _atomic_write(dest.with_suffix(".summary.json"), json.dumps(summary, ensure_ascii=False, indent=1))

    if incomplete and strict:
        raise IncompleteSnapshotError(incomplete)
    return SnapshotResult(path=dest, docs=len(lines), books=book_counts, incomplete=incomplete, duration_s=round(time.monotonic() - started, 3))


def _append_page(path: Path, docs: list[dict], book: str, fetched_at: str, page: int, label: str) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        for d in docs:
            line = {
                "source_id": d.get("_id"),
                "book": book,
                "fetched_at": fetched_at,
                "page": page,
                "label": label,
                "raw": d,
            }
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _num_key(n: str):
    return (0, (len(n), n)) if n.isdigit() else (1, n)


def _count_new(docs: list[dict], seen: set) -> int:
    """Count docs whose _id is not already tracked; updates `seen` in place."""
    n = 0
    for d in docs:
        sid = d.get("_id")
        if sid and sid not in seen:
            seen.add(sid)
            n += 1
    return n
