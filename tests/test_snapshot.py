# -*- coding: utf-8 -*-
"""Tests for corpus_sync.snapshot (Task 3).

Design decisions locked by live smoke (see tests/fixtures/smoke_range_note.md):
- Books with live count >= 10,000 (LOOKUP max_result_window) are split into one
  query per chapter; smaller books use a single book-scope query.
- Chapter scope values with double quotes use '*' in place of '"' (verified
  live: exact totals for even and odd quote counts; parseFieldScopes needs a
  quote-free value).
- Completeness = per-book unique-doc count vs /v1/browse/books live count.
- Shortfall repair for split books: number:"<n>" exact-term queries (number is
  a simple-value field -> term query) over the number seed set, descending.

All tests use FakeFetcher (dict-driven) — no live network.
"""

import json
import re
import time
from pathlib import Path

import pytest

from corpus_sync import config, http as http_mod, snapshot
from corpus_sync.fetch import Book

PER_PAGE = 10
MAX_PAGE = snapshot.MAX_PAGE_SIZE


def _doc(bid, n):
    return {"_id": bid, "number": str(n), "book": "b", "chapter": "c", "arabic": "a", "english": "e"}


class FakeFetcher:
    """Lookup engine over a dict of in-memory books.

    book_data: {book_name: {"chapters": {chapter: [doc, ...]}, "extra": [doc, ...]}}
    live_counts override the computed totals (to simulate corpus drift).
    """

    def __init__(self, book_data, live_counts=None):
        self.book_data = book_data
        self.live_counts = live_counts or {}
        self.calls = []

    def books(self):
        out = []
        for name, data in self.book_data.items():
            n = self.live_counts.get(name, sum(len(v) for v in data["chapters"].values()) + len(data["extra"]))
            out.append(Book(name=name, name_ar=name, slug=name, count=n))
        return out

    def facets(self, name):
        return {}

    def lookup(self, q, page, per_page=PER_PAGE):
        self.calls.append(q)
        m = re.search(r'book:"([^"]*)"', q)
        book = m.group(1) if m else None
        ch = re.search(r'chapter:"(.*)"\s*$', q)
        num = re.search(r'number:"([^"]*)"\s*$', q)
        data = self.book_data.get(book, {"chapters": {}, "extra": []})
        docs = []
        if ch:
            docs = list(data["chapters"].get(ch.group(1).replace("*", '"'), []))
        elif num:
            docs = [d for c in data["chapters"].values() for d in c if str(d["number"]) == num.group(1)]
            docs += [d for d in data["extra"] if str(d["number"]) == num.group(1)]
        else:
            docs = [d for c in data["chapters"].values() for d in c] + list(data["extra"])
        total = len(docs)
        start = (page - 1) * per_page
        return snapshot.LookupResult(total=total, collection=docs[start:start + per_page])


def _big_data():
    # 10,100 docs across 2 chapters -> split book (>= MAX_PAGE)
    return {
        "chapters": {
            "One": [_doc("big-1-%d" % i, i) for i in range(1, 9001)],
            "Two": [_doc("big-2-%d" % i, i) for i in range(9001, 10101)],
        },
        "extra": [],
    }


def _small_data(n=300, prefix="s"):
    # Distinct `prefix` per book: source_ids are globally unique in the live API,
    # and finalize dedupes by source_id (keep-last) — colliding ids would make
    # one book's docs overwrite another's on resume.
    return {"chapters": {"Ch": [_doc(f"{prefix}-{i}", i) for i in range(1, n + 1)]}, "extra": []}


def _big_split(numbers):
    return {
        "books": {
            "Big": {
                "chapters": [
                    {"name": "One", "local_count": 9000, "quote": False},
                    {"name": "Two", "local_count": 1100, "quote": False},
                ],
                "numbers": numbers,
            }
        }
    }


# --- planning ---------------------------------------------------------------


def test_plan_small_book_single_query():
    books = [Book("Small", "s", "small", 300)]
    split = {"books": {"Small": {"chapters": [{"name": "A", "local_count": 100, "quote": False}], "numbers": ["1"]}}}
    qs = snapshot.plan_queries(books, split)
    assert len(qs) == 1
    q = qs[0]
    assert q.kind == "book"
    assert q.book == "Small"
    assert q.chapter is None
    assert q.number is None
    assert snapshot.query_for(q) == 'book:"Small"'


def test_plan_split_book_one_query_per_chapter():
    books = [Book("Big", "b", "big", 10100)]
    split = {
        "books": {
            "Big": {
                "chapters": [
                    {"name": "One", "local_count": 9000, "quote": False},
                    {"name": 'Quoted "Name"', "local_count": 1100, "quote": True},
                ],
                "numbers": ["1"],
            }
        }
    }
    qs = snapshot.plan_queries(books, split)
    assert len(qs) == 2
    assert all(q.kind == "chapter" for q in qs)
    assert {q.chapter for q in qs} == {"One", 'Quoted "Name"'}
    # quote chars replaced by '*' so the scope value stays quote-free
    q = [x for x in qs if x.chapter.startswith("Quoted")][0]
    assert snapshot.query_for(q) == 'book:"Big" chapter:"Quoted *Name*"'


def test_plan_unknown_book_falls_back_to_book_query():
    books = [Book("Huge", "h", "huge", 12000)]
    qs = snapshot.plan_queries(books, {"books": {}})
    assert len(qs) == 1
    assert qs[0].kind == "book"


# --- run --------------------------------------------------------------------


def test_run_writes_jsonl_and_state(tmp_data_dir):
    fetcher = FakeFetcher({"Small": _small_data(300)})
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    assert res.incomplete == []
    assert res.path.name.endswith(".jsonl")
    lines = [json.loads(x) for x in res.path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 300
    assert set(lines[0]) == {"source_id", "book", "fetched_at", "page", "label", "raw"}
    assert lines[0]["raw"]["_id"] == "s-1"
    assert lines[0]["label"] == "book:Small"
    assert lines[0]["page"] == 1
    # summary sidecar
    summary = json.loads(res.path.with_suffix(".summary.json").read_text(encoding="utf-8"))
    assert summary["incomplete"] == []
    assert summary["docs"] == 300
    # state file with per-label completed pages
    state = json.loads((tmp_data_dir / "state" / "snapshot_state.json").read_text(encoding="utf-8"))
    assert state["snapshot"] == res.path.name
    assert state["completed"]["book:Small"] == list(range(1, 31))
    assert set(state["sha256"]) and len(state["sha256"]) == 64
    # exactly one jsonl in RAW_DIR (the snapshot file itself)
    assert [p.name for p in (tmp_data_dir / "raw").glob("*.jsonl")] == [res.path.name]


def test_dedupe_keeps_last_and_uses_book_total(tmp_data_dir):
    d1 = _doc("dup-1", 1)
    d2 = _doc("dup-1", 2)  # same _id, later in order -> kept
    data = {"chapters": {"A": [d1], "B": [d2]}, "extra": []}
    fetcher = FakeFetcher({"Big": data}, live_counts={"Big": 1})
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    lines = [json.loads(x) for x in res.path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 1
    assert lines[0]["raw"]["number"] == "2"
    # book unique count == live count -> complete despite chapter over-match
    assert res.incomplete == []


def test_incomplete_book_reported(tmp_data_dir):
    # 300 live, but the only query finds 290 -> book reported incomplete
    fetcher = FakeFetcher({"Small": _small_data(290)}, live_counts={"Small": 300})
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    assert res.incomplete == ["Small"]
    # non-strict run: result returned, incomplete in summary
    summary = json.loads(res.path.with_suffix(".summary.json").read_text(encoding="utf-8"))
    assert summary["incomplete"] == ["Small"]
    # strict run raises
    with pytest.raises(snapshot.IncompleteSnapshotError):
        snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE, strict=True)


def test_resume_skips_completed(tmp_data_dir):
    fetcher1 = FakeFetcher({"Small": _small_data(300)})
    res1 = snapshot.run_snapshot(dest=None, fetcher=fetcher1, clock=lambda: "T1", per_page=PER_PAGE)
    dest = res1.path
    content1 = dest.read_text(encoding="utf-8")

    fetcher2 = FakeFetcher({"Small": _small_data(300)})
    res2 = snapshot.run_snapshot(dest=dest, fetcher=fetcher2, clock=lambda: "T2", per_page=PER_PAGE)
    assert res2.path == dest
    # first run made 30 pages; resume re-fetches nothing
    assert fetcher2.calls == []
    # file untouched
    assert dest.read_text(encoding="utf-8") == content1
    assert res2.incomplete == []


def test_resume_partial_fetch_continues(tmp_data_dir):
    fetcher = FakeFetcher({"Small": _small_data(300)})
    res1 = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    dest = res1.path
    # simulate crash after page 1: drop later pages from file and state
    state_p = tmp_data_dir / "state" / "snapshot_state.json"
    state = json.loads(state_p.read_text(encoding="utf-8"))
    state["completed"] = {"book:Small": [1]}
    state_p.write_text(json.dumps(state), encoding="utf-8")
    lines = [json.loads(x) for x in dest.read_text(encoding="utf-8").splitlines() if x.strip()]
    kept = [l for l in lines if l["page"] == 1]
    dest.write_text("".join(json.dumps(l, ensure_ascii=False) + "\n" for l in kept), encoding="utf-8")

    fetcher2 = FakeFetcher({"Small": _small_data(300)})
    res2 = snapshot.run_snapshot(dest=dest, fetcher=fetcher2, clock=lambda: "T2", per_page=PER_PAGE)
    # pages 2..30 re-fetched (29 pages)
    assert len(fetcher2.calls) == 29
    lines = [json.loads(x) for x in dest.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 300
    assert res2.incomplete == []


def test_repair_pass_covers_missing_documents(tmp_data_dir):
    # split book; 100 docs live in "extra" that no chapter query returns
    extra = [_doc("big-x-%d" % i, 20000 + i) for i in range(1, 101)]
    numbers = [str(20000 + i) for i in range(1, 101)] + [str(i) for i in range(1, 11)]
    fetcher = FakeFetcher({"Big": dict(_big_data(), extra=extra)}, live_counts={"Big": 10200})
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE, split=_big_split(numbers))
    assert res.incomplete == []
    lines = [json.loads(x) for x in res.path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 10200
    # repair used number-term queries
    repair_calls = [c for c in fetcher.calls if 'number:"' in c]
    assert len(repair_calls) >= 100
    assert all(c.startswith('book:"Big" number:') for c in repair_calls)


def test_repair_cannot_covers_stays_incomplete(tmp_data_dir):
    # shortfall of 100 but only 40 recoverable docs -> book stays incomplete
    extra = [_doc("big-x-%d" % i, 20000 + i) for i in range(1, 41)]
    numbers = [str(20000 + i) for i in range(1, 101)]
    fetcher = FakeFetcher({"Big": dict(_big_data(), extra=extra)}, live_counts={"Big": 10200})
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE, split=_big_split(numbers))
    assert res.incomplete == ["Big"]


# --- completeness ------------------------------------------------------------


def test_verify_completeness_lists_mismatches():
    live = [Book("A", "a", "a", 5), Book("B", "b", "b", 7), Book("C", "c", "c", 9)]
    counts = {"A": 5, "B": 6, "D": 9}  # B short, C missing, D extra
    bad = snapshot.verify_completeness(counts, live)
    assert sorted(bad) == ["B", "C", "D"]
    assert snapshot.verify_completeness({"A": 5, "B": 7, "C": 9}, live) == []


# --- query construction ------------------------------------------------------


def test_query_for_each_kind():
    q = snapshot.Query(book="B", chapter=None, label="book:B", expected_count=0, kind="book")
    assert snapshot.query_for(q) == 'book:"B"'
    q = snapshot.Query(book="B", chapter="The Rare Ahadith", label="x", expected_count=0, kind="chapter")
    assert snapshot.query_for(q) == 'book:"B" chapter:"The Rare Ahadith"'
    q = snapshot.Query(book="B", chapter='"Allah sent down His comfort..." (9:40)', label="x", expected_count=0, kind="chapter")
    assert snapshot.query_for(q) == 'book:"B" chapter:"*Allah sent down His comfort...* (9:40)"'
    q = snapshot.Query(book="B", chapter="One", label="x", expected_count=0, kind="number", number="571")
    assert snapshot.query_for(q) == 'book:"B" number:"571"'


# --- page-level retry (T10 live-run hardening) ---------------------------------


class _FlakyFetcher:
    """Wraps a fetcher; raises HttpError on the first N lookup calls."""

    def __init__(self, inner, fail_first=1):
        self.inner = inner
        self.fail_first = fail_first
        self.n = 0

    def books(self):
        return self.inner.books()

    def lookup(self, q, page, per_page=PER_PAGE):
        self.n += 1
        if self.n <= self.fail_first:
            raise http_mod.HttpError("https://rewayaat.info/v1/narrations", self.n)
        return self.inner.lookup(q, page, per_page)


class _BookFailFetcher:
    """FakeFetcher that fails every lookup for one specific book."""

    def __init__(self, book_data, fail_book):
        self.inner = FakeFetcher(book_data)
        self.fail_book = fail_book
        self.calls = []

    def books(self):
        return self.inner.books()

    def lookup(self, q, page, per_page=PER_PAGE):
        self.calls.append(q)
        if f'book:"{self.fail_book}"' in q:
            raise http_mod.HttpError("https://rewayaat.info/v1/narrations", 1)
        return self.inner.lookup(q, page, per_page)


def test_page_http_error_retried_and_recovers(tmp_data_dir, monkeypatch):
    # One transient read-timeout on the first page must not kill the run.
    monkeypatch.setattr(time, "sleep", lambda s: None)
    fetcher = _FlakyFetcher(FakeFetcher({"Small": _small_data(300)}), fail_first=1)
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    assert res.incomplete == []
    assert res.failed == []
    lines = [json.loads(x) for x in res.path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 300
    # 30 pages + 1 extra call for the retried first page
    assert fetcher.n == 31


def test_failed_query_is_isolated_run_continues(tmp_data_dir, monkeypatch):
    # A book whose first page exhausts ALL retries must not kill the run:
    # it is recorded as failed + incomplete while the other book completes.
    monkeypatch.setattr(time, "sleep", lambda s: None)
    fetcher = _BookFailFetcher(
        {"Small": _small_data(300), "Broken": _small_data(200, prefix="br")}, "Broken")
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    assert res.incomplete == ["Broken"]
    assert res.books == {"Small": 300}
    assert res.failed == ["book:Broken"]
    lines = [json.loads(x) for x in res.path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert {l["book"] for l in lines} == {"Small"}
    assert len(lines) == 300
    # the failed label is persisted in state for the next --resume
    state = json.loads(
        (config.STATE_DIR / snapshot.STATE_FILE).read_text(encoding="utf-8"))
    assert state["failed"] == ["book:Broken"]


def test_failed_query_retried_on_resume(tmp_data_dir, monkeypatch):
    # The failed query is re-attempted on resume; if the failure was
    # transient the book completes and `failed` is cleared.
    monkeypatch.setattr(time, "sleep", lambda s: None)
    fail_first = {"v": True}

    class _TransientFetcher(_BookFailFetcher):
        def lookup(self, q, page, per_page=PER_PAGE):
            if fail_first["v"] and f'book:"Broken"' in q:
                raise http_mod.HttpError("u", 1)
            return self.inner.lookup(q, page, per_page)

    f1 = _TransientFetcher(
        {"Small": _small_data(300), "Broken": _small_data(200, prefix="br")}, "Broken")
    res1 = snapshot.run_snapshot(dest=None, fetcher=f1, clock=lambda: "T1", per_page=PER_PAGE)
    assert res1.failed == ["book:Broken"]
    dest = res1.path

    fail_first["v"] = False  # API recovered
    f2 = _TransientFetcher(
        {"Small": _small_data(300), "Broken": _small_data(200, prefix="br")}, "Broken")
    res2 = snapshot.run_snapshot(dest=dest, fetcher=f2, clock=lambda: "T2", per_page=PER_PAGE)
    assert res2.incomplete == []
    assert res2.failed == []
    assert res2.books == {"Small": 300, "Broken": 200}
    assert res2.docs == 500
    state = json.loads(
        (config.STATE_DIR / snapshot.STATE_FILE).read_text(encoding="utf-8"))
    assert state["failed"] == []


def test_failed_page_of_multi_page_query_isolated(tmp_data_dir, monkeypatch):
    # Page 2 of a 30-page query (per_page=10) fails permanently; the remaining
    # pages still fetch, the failed page is recorded, and the run finishes
    # (book stays short).
    monkeypatch.setattr(time, "sleep", lambda s: None)

    class _Page2Fail(FakeFetcher):
        def lookup(self, q, page, per_page=PER_PAGE):
            if page == 2:
                raise http_mod.HttpError("https://rewayaat.info/v1/narrations", 1)
            return super().lookup(q, page, per_page)

    fetcher = _Page2Fail({"Small": _small_data(300)})
    res = snapshot.run_snapshot(dest=None, fetcher=fetcher, clock=lambda: "T1", per_page=PER_PAGE)
    assert res.incomplete == ["Small"]
    assert res.failed == ["book:Small"]
    lines = [json.loads(x) for x in res.path.read_text(encoding="utf-8").splitlines() if x.strip()]
    # 300 live docs; all pages landed except page 2 (10 docs missing)
    assert len(lines) == 290
    state = json.loads(
        (config.STATE_DIR / snapshot.STATE_FILE).read_text(encoding="utf-8"))
    assert state["completed"]["book:Small"] == [p for p in range(1, 31) if p != 2]
    # failed is cleared at run start and re-recorded when the failure recurs
    assert state["failed"] == ["book:Small"]


def test_failed_query_retried_when_api_recovers_on_resume(tmp_data_dir, monkeypatch):
    # Cover the resume path explicitly: after a failed run, resume re-attempts
    # the failed query (not skipped) and completes the book.
    monkeypatch.setattr(time, "sleep", lambda s: None)
    f1 = _BookFailFetcher(
        {"Small": _small_data(300), "Broken": _small_data(200, prefix="br")}, "Broken")
    res1 = snapshot.run_snapshot(dest=None, fetcher=f1, clock=lambda: "T1", per_page=PER_PAGE)
    assert res1.failed == ["book:Broken"]
    # resume: fetcher no longer fails; the previously-failed query must run.
    f2 = FakeFetcher({"Small": _small_data(300), "Broken": _small_data(200, prefix="br")})
    res2 = snapshot.run_snapshot(dest=res1.path, fetcher=f2, clock=lambda: "T2", per_page=PER_PAGE)
    assert res2.incomplete == []
    assert res2.failed == []
    assert res2.books == {"Small": 300, "Broken": 200}
