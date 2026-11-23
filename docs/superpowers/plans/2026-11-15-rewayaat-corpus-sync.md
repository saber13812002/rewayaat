# Rewayaat Corpus Sync Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A permanent, automated pipeline that snapshots the live Rewayaat corpus via the public REST API, detects document/field/sentence-level changes (Arabic + English), produces monthly analytics, and publishes a versioned Hugging Face dataset — run weekly (and monthly) by GitHub Actions on the user's fork.

**Architecture:** A pure-Python package `corpus_sync/` (stdlib + requests/pandas/pyarrow/diff-match-patch/huggingface_hub). Each run: (1) enumerate books + scoped lookup queries, (2) page through LOOKUP results (full 31-field docs, no per-id fetches), (3) write an append-only raw JSONL snapshot with a resumable checkpoint, (4) validate completeness against `/v1/browse/books` totals, (5) normalize to a stable schema with per-field sha256 hashes, (6) diff against the previous *complete* snapshot in SQLite, (7) compute monthly metrics, (8) stage + upload the HF dataset (private). No Elasticsearch access (user decision: API-only). State (SQLite + checkpoint + last raw pointer) round-trips as GitHub Actions artifacts.

**Tech Stack:** Python 3.13 (64-bit — local path `C:\Users\s.tabatabaei\AppData\Local\Programs\Python\Python313\python.exe`, GitHub `actions/setup-python@v5` 3.13), requests, pandas, pyarrow, diff-match-patch, huggingface_hub, pytest, SQLite (stdlib), GitHub Actions.

**Spec:** User master prompt (10 phases, absolute rules, final report A–J) + verified source-discovery evidence in `docs/corpus-sync/01-source-discovery.md` (counts, endpoints, limits, strategy). The discovery report is normative for all API behavior.

## Global Constraints

- **Interpreter:** 64-bit Python 3.13 only. The system `python` on the dev machine is 32-bit 3.14 *without* pandas/pyarrow wheels. Every local command in this plan uses `PY = "C:\Users\s.tabatabaei\AppData\Local\Programs\Python\Python313\python.exe"` (in tests/workflows: `python3.13` / `python`).
- **Source:** `https://rewayaat.info` REST API only (no ES, per user decision 2026-11-15). No credentials needed for the API; the HF token is the *only* secret, read from env `HF_TOKEN`, never written to code, logs, reports, or the dataset.
- **HF visibility:** dataset repo stays **private** until a data license is confirmed (none found in the repo — absolute rule).
- **Deletion rule:** a doc absent from a page is NOT a deletion. REMOVED is reported only when the new snapshot is *complete* (per-book counts match `/v1/browse/books`) AND the doc was in a *complete* previous snapshot.
- **10,000-hit cap:** every LOOKUP query must be scoped to ≤10,000 hits. Books ≤10,000: one query `book:"<exact name>"`. Books >10,000 (Al-Kāfi 14,242; Ghurar al-Ḥikam 11,012): one query per chapter `book:"X" chapter:"<Y>"` (max chapter = 389 docs, verified). `totalResultSetSize` is clamped at 10,000 for unscoped queries — never trust it for >10k books; completeness = sum of scoped sub-query counts vs book count.
- **Lucene-wildcard unsafe chapters:** 44 chapters in the two >10k books contain `? [ ] * " ' ^ + \ { }` (list computed by `tmp/unsafe_chapters.py`, committed as fixture `tests/fixtures/unsafe_chapters.json`). `chapter:` scoping is a *wildcard* filter server-side (`*value*`), so such chapters need the fallback: query the whole book with `sort_fields=book,number` paging is impossible (>10k), so for unsafe chapters fetch by **client-side filtering over number ranges**: page the book with repeated `book:"X"` queries using `per_page=10000` is also impossible — therefore: for each unsafe chapter, query `book:"X"` **with the chapter value's wildcard-escaped form** is NOT supported by the server; the fallback is **per-chapter via `q` search on the exact chapter text**: `book:"X" "<exact chapter title>"` in SEARCH mode is relevance-ordered and capped — **Decision: fallback = book-level fetch with `number` ranges.** `number` is a simple (term) field; query `book:"X" number:[1 TO 5000]` etc. (Lucene range syntax accepted by query_string over the keyword field — verify in Task 3 Step 1 against live API before wiring; if range syntax fails, fall back to paging `book:"X"` with `sort_fields=book,number` at `per_page=10000` in 10k offset chunks and client-side chapter filter — both stay within cap per chunk). This fallback is exercised by tests with a fake transport and by one live smoke check in Task 3.
- **Retry/timeout:** per-request timeout 240 s; 4 attempts; exponential backoff 5 s → 40 s + jitter; 1 concurrent request; sustained rate ≤ 2 req/s (token bucket). Observed live: identical query 1.2 s vs >120 s; transient DNS `getaddrinfo failed`.
- **Idempotency & resumability:** snapshot state file `data/state/snapshot_state.json` (completed queries + pages, sha256 of raw file); re-running resumes and never double-writes (dedupe by `source_id` on finalize). No concurrent syncs: single process lock file + GH Actions `concurrency`.
- **Windows console:** never print non-ASCII to stdout (cp1252); all human-readable output to UTF-8 files.
- **No test data as real:** everything under `tests/fixtures/` carries a manifest (`fixture_manifest.json`: source URL, fetched_at, count, sha256). Reports never count fixture docs.
- **Every reported number is measured** (counts/hashes from stored data), never copied from docs.

## Review Focus

1. **Chapter names with Lucene special chars** (`?`, `[`, `"`, `*` — 44 of them) → server wildcard scope over/under-matches → Task 3: exact-match fallback + per-chapter count reconciliation against `/v1/browse/facets` counts; mismatch = hard error naming the chapter.
2. **Slow/hung requests & transient DNS failures** (observed live) → Task 1: 240 s timeout, 4 retries, backoff+jitter, rate limit; Task 3: checkpoint after every page, resume re-issues only missing pages; a killed run must not lose data.
3. **`totalResultSetSize` clamped at 10,000** for >10k books → Task 3: completeness computed from scoped sub-query totals, never from a single unscoped total.
4. **Punctuation/diacritic drift in Arabic/English text** → Task 6: sentence diff compares punctuation-stripped forms for *matching*, reports counts on original sentences; punctuation-only edits must yield 0 modified sentences.
5. **Incomplete snapshot at detection time** (a book's pages failed) → Task 7: `detect` raises `IncompleteSnapshotError` and writes a `missing` list instead of emitting REMOVED rows.

## File Structure

```
corpus_sync/
  __init__.py
  config.py          # constants: URLs, limits, paths, norm version, HF repo id
  http.py            # session, retry/backoff/jitter, rate limit, get_json/get_text
  fetch.py           # books(), facets(name), lookup(q,page,per_page), hadith(id)
  snapshot.py        # plan_queries(), run_snapshot() w/ checkpoint + completeness
  normalize.py       # normalize_doc(), field_hash(), split_sentences()
  store.py           # SQLite schema + snapshot/record/change ops
  diff.py            # sentence_diff() (difflib + diff-match-patch)
  detect.py          # detect_changes(old,new) -> ChangeSet
  analytics.py       # monthly_metrics(), cumulative(), render_report()
  publish.py         # build_dataset_dir(), upload_dataset()
  cli.py             # argparse: snapshot/detect/analyze/publish/validate
  requirements.txt
tests/
  conftest.py        # PY interpreter, fake transport, tmp data dir
  fixtures/          # live-captured JSON (books, lookup page, by-id doc,
                     # unsafe_chapters.json, fixture_manifest.json)
  test_http.py  test_fetch.py  test_snapshot.py  test_normalize.py
  test_store.py  test_diff.py  test_detect.py  test_analytics.py
  test_publish.py  test_e2e.py
.github/workflows/corpus-sync.yml
docs/corpus-sync/    # 01-source-discovery.md (exists) + data-dictionary.md,
                     # recovery-guide.md, pipeline-README.md, report-<month>.md
data/                # gitignored working dir (raw/, db/, normalized/, state/, out/)
```

---

### Task 1: Scaffolding + HTTP client (retry, backoff, rate limit, timeout)

**Files:**
- Create: `corpus_sync/__init__.py`, `corpus_sync/config.py`, `corpus_sync/http.py`, `corpus_sync/requirements.txt`
- Test: `tests/conftest.py`, `tests/test_http.py`

**Interfaces:**
- Produces:
  - `config.py`: `BASE_URL: str`, `PER_PAGE: int = 200`, `REQUEST_TIMEOUT_S: int = 240`, `MAX_RETRIES: int = 4`, `MIN_BACKOFF_S: float = 5.0`, `RATE_LIMIT_PER_S: float = 2.0`, `DATA_DIR: Path = Path("data")`, `DB_PATH: Path`, `RAW_DIR: Path`, `STATE_DIR: Path`, `NORM_VERSION: str = "1.0"`, `HF_REPO_ID: str = "saber13812002/rewayaat-corpus"`, `HF_IS_PRIVATE: bool = True`
  - `http.get_json(url: str, *, params: dict | None = None) -> object`
  - `http.get_text(url: str, *, params: dict | None = None) -> str`
  - `http.HttpError(Exception)` with `.status: int | None` and `.attempts: int`
- Consumes: nothing.

- [ ] **Step 1: Write failing tests** — `tests/test_http.py`:

```python
import pytest
from corpus_sync import http

def test_retries_then_succeeds(monkeypatch):
    calls = []
    def flaky(url, params=None):
        calls.append(url)
        if len(calls) < 3:
            raise http.TransportError("boom")
        return b'{"ok": true}'
    monkeypatch.setattr(http, "_do_request", flaky)
    monkeypatch.setattr(http, "_sleep", lambda s: None)
    assert http.get_json("https://x/y") == {"ok": True}
    assert len(calls) == 3

def test_raises_httpererror_after_max_retries(monkeypatch):
    monkeypatch.setattr(http, "_do_request", lambda u, p=None: (_ for _ in ()).throw(http.TransportError("down")))
    monkeypatch.setattr(http, "_sleep", lambda s: None)
    with pytest.raises(http.HttpError) as e:
        http.get_json("https://x/y")
    assert e.value.attempts == http.MAX_RETRIES

def test_404_is_not_retried(monkeypatch):
    calls = []
    def gone(url, params=None):
        calls.append(url)
        raise http.HttpStatusError(404, b"not found")
    monkeypatch.setattr(http, "_do_request", gone)
    monkeypatch.setattr(http, "_sleep", lambda s: None)
    with pytest.raises(http.HttpError) as e:
        http.get_json("https://x/y")
    assert e.value.status == 404 and len(calls) == 1   # 4xx (except 429/408) no retry

def test_rate_limit_spacing(monkeypatch):
    sleeps = []
    monkeypatch.setattr(http, "_do_request", lambda u, p=None: b"{}")
    monkeypatch.setattr(http, "_sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(http.time, "monotonic", fake_clock)
    http.get_json("https://x/1"); http.get_json("https://x/2")
    assert any(s >= 0.4 for s in sleeps)   # 2 req/s => >=0.5s spacing minus epsilon
```
(Plus `fake_clock` helper in the test file: a callable returning 0, 0.1, 0.2… on call.)

- [ ] **Step 2: Run to verify failure**

Run: `PY -m pytest tests/test_http.py -v` → FAIL (module not found).

- [ ] **Step 3: Implement** `corpus_sync/config.py` (plain constants as in Interfaces) and `corpus_sync/http.py`:
  - `requests.Session` with `User-Agent: rewayaat-corpus-sync/1.0`; `_do_request` = `session.get(url, params=params, timeout=config.REQUEST_TIMEOUT_S)` raising `TransportError` on `requests.RequestException` and `HttpStatusError(status, body)` on `r.status_code >= 400` (body kept for 5xx diagnostics, never logged in full — first 200 chars only).
  - `get_json`/`get_text` loop: attempts 1..`MAX_RETRIES`; retry on `TransportError`, 429, 408, 5xx; sleep = `min(60, MIN_BACKOFF_S * 2**(attempt-1)) + random.uniform(0, 1)` via `_sleep`; token bucket: if `now - last < 1/RATE_LIMIT_PER_S` sleep the difference.
- [ ] **Step 4: Run to verify pass** — `PY -m pytest tests/test_http.py -v` → PASS (5 tests).
- [ ] **Step 5: Commit** — `git add corpus_sync/ tests/ && git commit -m "feat(corpus-sync): scaffold package + resilient http client"`.

---

### Task 2: Fetch layer (books, facets, lookup, by-id)

**Files:**
- Create: `corpus_sync/fetch.py`
- Create: `tests/fixtures/books.json` (copy of `tmp/browse_books.json`), `tests/fixtures/lookup_khisal_p1.json` (copy of `tmp/lookup_shape.json`), `tests/fixtures/fixture_manifest.json`
- Test: `tests/test_fetch.py`

**Interfaces:**
- Consumes: `http.get_json`, `config.BASE_URL`.
- Produces:
  - `fetch.books() -> list[Book]` — `@dataclass Book(name: str, name_ar: str, slug: str, count: int)` from `GET /v1/browse/books` (fields `name`, `nameAr`, `slug`, `count`).
  - `fetch.facets(book_name: str) -> dict` — `GET /v1/browse/facets?book=<exact name>`; returns parsed JSON (`{"book", "facets": {"chapter": [{"name","count"}...], "volume": [...], ...}, "primaryFacet"}`).
  - `fetch.lookup(query: str, page: int, per_page: int = config.PER_PAGE) -> LookupResult` — `@dataclass LookupResult(total: int, collection: list[dict])` from `GET /v1/narrations` with `params={"q": query, "sort_fields": "book,number", "page": page, "per_page": per_page}`; `total = resp["totalResultSetSize"]`, `collection = resp["collection"]`.
  - `fetch.hadith(source_id: str) -> dict` — `GET /v1/narrations/{id}` → `resp["narration"]`.

- [ ] **Step 1: Write fixtures + failing tests** — copy the two live-captured JSON files into `tests/fixtures/`; write `fixture_manifest.json` listing each file's source URL, fetched_at `2026-11-15`, sha256. `tests/test_fetch.py`:

```python
def test_books_parses_catalog(monkeypatch):
    raw = open("tests/fixtures/books.json", "rb").read()
    monkeypatch.setattr("corpus_sync.http.get_json", lambda url, params=None: json.loads(raw))
    books = fetch.books()
    assert [b.count for b in books] == [1469, 1281, 14242, 574, 45, 11012, ...]  # full list, sum 50038
    assert sum(b.count for b in books) == 50038

def test_lookup_maps_response(monkeypatch):
    raw = open("tests/fixtures/lookup_khisal_p1.json", "rb").read()
    monkeypatch.setattr("corpus_sync.http.get_json", lambda url, params=None: json.loads(raw))
    res = fetch.lookup('book:"Al-Khiṣāl"', 1, per_page=3)
    assert res.total == 1281 and len(res.collection) == 3
    assert res.collection[0]["_id"] == "Al-Khisal-Saduq:1282"

def test_lookup_uses_lookup_mode_params(monkeypatch):
    seen = {}
    def cap(url, params=None):
        seen.update(params or {}); return {"totalResultSetSize": 0, "collection": []}
    monkeypatch.setattr("corpus_sync.http.get_json", cap)
    fetch.lookup('book:"X"', 2, per_page=200)
    assert seen["sort_fields"] == "book,number" and seen["page"] == 2 and seen["per_page"] == 200
```

- [ ] **Step 2: Run to verify failure** — `PY -m pytest tests/test_fetch.py -v` → FAIL (no module).
- [ ] **Step 3: Implement** `corpus_sync/fetch.py` per Interfaces (thin mapping, no logic).
- [ ] **Step 4: Run to verify pass** — `PY -m pytest tests/test_fetch.py -v` → PASS (3 tests).
- [ ] **Step 5: Commit** — `git add corpus_sync/fetch.py tests/ && git commit -m "feat(corpus-sync): fetch layer for books/facets/lookup/by-id"`.

---

### Task 3: Snapshot orchestrator (query plan, chapter split, unsafe-chapter fallback, checkpoint, completeness)

**Files:**
- Create: `corpus_sync/snapshot.py`
- Create: `tests/fixtures/unsafe_chapters.json` (output of `tmp/unsafe_chapters.py`: `{book: [chapter names]}`)
- Test: `tests/test_snapshot.py`

**Interfaces:**
- Consumes: `fetch`, `config`, `http`.
- Produces:
  - `snapshot.plan_queries(books: list[Book], facets_map: dict[str, dict]) -> list[Query]` — `@dataclass Query(book: str, chapter: str | None, label: str, expected_count: int)`. Books ≤10,000 → one query (chapter=None, expected=book.count). Books >10,000 → one query per chapter from `facets_map[book]["facets"]["chapter"]` (expected=chapter count); chapters in `tests/fixtures/unsafe_chapters.json` get `chapter=None` plus `number_range=(lo,hi)` (computed from local batches: contiguous `number` spans per unsafe chapter — precomputed once, committed as part of `unsafe_chapters.json` with `ranges` per chapter).
  - `snapshot.run_snapshot(*, dest: Path | None = None, fetcher: Fetcher = None, clock: Callable = None) -> SnapshotResult`
    - `@dataclass SnapshotResult(path: Path, docs: int, books: dict[str, int], incomplete: list[str], duration_s: float)`
    - `Fetcher` protocol = any object with `.books()`, `.facets(name)`, `.lookup(q,page,per_page)`.
    - Writes `<RAW_DIR>/<YYYYMMDD-HHMMSS>.jsonl`, one line per doc: `{"source_id": ..., "book": ..., "fetched_at": <iso8601 utc>, "raw": <full doc as returned>}`.
    - After each completed (query, page) → rewrites `STATE_DIR/snapshot_state.json` `{"snapshot": <filename>, "completed": {query.label: [pages...]}, "sha256": ...}` (atomic: temp file + replace).
    - Resume: if a state file exists for an in-progress snapshot, skip completed (query,page) pairs.
    - Finalization: dedupe by `source_id` (keep last), verify per-book counts == `fetch.books()` counts; mismatches or failed queries → listed in `incomplete`; raises `IncompleteSnapshotError` (defined here) only when the CLI asked for strict mode (flag `strict=True` default False).
  - `snapshot.verify_completeness(conn_or_state, live_books) -> list[str]` (missing/mismatched book names).

- [ ] **Step 1: Live smoke of the number-range fallback (one-off, recorded)** — in a scratch script (kept as `tests/fixtures/smoke_range_note.md` with URL + timestamp + output), verify against live API:
  `q=book:"Al-Kāfi" number:[1 TO 5000]` (LOOKUP mode, per_page=2) → check `totalResultSetSize` is sane (≤5000, >0) and docs' numbers are within range. If range syntax is rejected (500), record the failure and switch the fallback to 10k-offset chunks with client-side chapter filtering (document which in the note). This single live check decides `plan_queries` behavior; the recorded output goes in the fixture note.
- [ ] **Step 2: Write failing tests** — `tests/test_snapshot.py` with a `FakeFetcher` (dict-driven: books, facets, pages):

```python
def test_plan_splits_big_books_by_chapter():
    books = [Book("Small", "", "small", 1281), Book("Big", "", "big", 14242)]
    facets = {"Big": {"facets": {"chapter": [{"name": "A", "count": 100}, {"name": "B", "count": 300}]}}}
    qs = snapshot.plan_queries(books, facets)
    assert [q.label for q in qs] == ["book:Small", "book:Big chapter:A", "book:Big chapter:B"]
    assert qs[0].chapter is None and qs[1].chapter == "A"

def test_plan_uses_ranges_for_unsafe_chapters():
    # unsafe chapter "How Are You?" must produce a number-range query, not a chapter wildcard
    ...

def test_run_writes_jsonl_and_state():
    res = snapshot.run_snapshot(fetcher=FakeFetcher(two_books=...))
    assert res.docs == 150 and set(res.books) == {"Small", "Big"}
    lines = [json.loads(l) for l in open(res.path, encoding="utf-8")]
    assert {k for k in lines[0]} == {"source_id", "book", "fetched_at", "raw"}
    assert (STATE_DIR / "snapshot_state.json").exists()

def test_resume_skips_completed_pages():
    # run, delete 20 docs from raw, run again with same fetcher -> count restored, no dup source_ids
    ...

def test_incomplete_book_reported_not_silent():
    # FakeFetcher raises on Big page 2 -> res.incomplete == ["Big"], docs only for completed pages
    ...
```
- [ ] **Step 3: Implement** `corpus_sync/snapshot.py`. Page loop: `page = 1; while (page-1)*per_page < expected_count:` fetch, append, checkpoint. `expected_count` from plan (facets counts); stop early when `len(collection) < per_page` AND `total` matches expected (guard against extra/missing). Dedupe at finalize.
- [ ] **Step 4: Run to verify pass** — `PY -m pytest tests/test_snapshot.py -v` → PASS (≥5 tests).
- [ ] **Step 5: Commit** — `git add corpus_sync/snapshot.py tests/ && git commit -m "feat(corpus-sync): snapshot orchestrator with checkpoint + completeness"`.

---

### Task 4: Normalization (stable schema, per-field hashes, rules v1)

**Files:**
- Create: `corpus_sync/normalize.py`
- Create: `tests/fixtures/hadith_alamali_1.json` (live by-id capture — re-fetch `GET /v1/narrations/Al-Amali-Saduq:1` once, save `narration` object + manifest entry)
- Test: `tests/test_normalize.py`

**Interfaces:**
- Consumes: `config.NORM_VERSION`.
- Produces:
  - `normalize.field_hash(text: str | None) -> str` — `""` → `sha256(b"")`; else `sha256(NFKC(text).strip().encode("utf-8"))`. NFKC+trim is the whole of rules v1 (documented; do not collapse whitespace — it changes text).
  - `normalize.normalize_doc(doc: dict, *, fetched_at: str) -> dict` — returns:
    `source_id` (=`doc["_id"]`), `book_id` (slug, passed in via `book_slug` kwarg), `book` (=`doc["book"]`), `chapter` (str, `""` if absent), `hadith_number` (str of `doc["number"]`), `narrator` (last entry of `doc.get("chain_people")` that is a str, else `""`), `arabic` (str or `""`), `english` (str or `""`), `metadata` (dict: everything else from `doc` minus `arabic`/`english`/`_id` — kept verbatim, JSON-serializable), `fetched_at`, `record_hash` = `sha256(canonical_json({source_id,book,chapter,hadith_number,arabic,english,metadata}))` where canonical_json = `json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",",":"))`, `arabic_hash`/`english_hash`/`metadata_hash` = `field_hash` of the respective canonical forms, `normalization_version` = `NORM_VERSION`, `raw` (the original doc dict).
  - `normalize.split_sentences(text: str, lang: str) -> list[str]` — `lang="ar"`: split on `[،؛\n]|(?<=[.!?؟])\s+`; `lang="en"`: split on `\n|(?<=[.!?])\s+`; strip each; drop empties.

- [ ] **Step 1: Write failing tests** — `tests/test_normalize.py`:

```python
def test_field_hash_stable_and_punct_insensitive_only_via_nfc():
    assert normalize.field_hash("salam") == normalize.field_hash("salam")
    assert normalize.field_hash(None) == normalize.field_hash("")
    assert normalize.field_hash(" a b ") == normalize.field_hash("a b")

def test_normalize_doc_full_schema():
    doc = json.load(open("tests/fixtures/hadith_alamali_1.json", encoding="utf-8"))
    rec = normalize.normalize_doc(doc, book_slug="al-amali", fetched_at="2026-11-15T00:00:00Z")
    assert rec["source_id"] == "Al-Amali-Saduq:1"
    assert rec["book_id"] == "al-amali"
    assert rec["normalization_version"] == "1.0"
    assert set(rec) == {"source_id","book_id","book","chapter","hadith_number","narrator",
                        "arabic","english","metadata","fetched_at","record_hash","arabic_hash",
                        "english_hash","metadata_hash","normalization_version","raw"}
    assert rec["raw"]["_id"] == "Al-Amali-Saduq:1"
    assert rec["record_hash"] != rec["arabic_hash"]

def test_record_hash_changes_with_any_content_change():
    base = {...}   # minimal doc
    r1 = normalize.normalize_doc(base, book_slug="s", fetched_at="t")
    r2 = normalize.normalize_doc({**base, "english": base["english"] + "."}, book_slug="s", fetched_at="t")
    assert r1["record_hash"] != r2["record_hash"] and r1["english_hash"] != r2["english_hash"]

def test_split_sentences_arabic_and_english():
    assert normalize.split_sentences("أ. ب، ج؛ د. هـ", "ar") == ["أ", "ب،", "ج؛", "د", "هـ"]  # exact per spec regex; pin actual behavior
    assert normalize.split_sentences("One. Two? Three!\nFour", "en") == ["One", "Two?", "Three!", "Four"]
```
(Implementer: pin the Arabic regex's *actual* output in the test — the assertion above is the intent; adjust the expected list to the spec's regex result and comment the rule.)
- [ ] **Step 2: Run to verify failure** — `PY -m pytest tests/test_normalize.py -v` → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/normalize.py`.
- [ ] **Step 4: Run to verify pass** — `PY -m pytest tests/test_normalize.py -v` → PASS.
- [ ] **Step 5: Commit** — `git add corpus_sync/normalize.py tests/ && git commit -m "feat(corpus-sync): normalization schema v1.0 with per-field hashes"`.

---

### Task 5: SQLite store (snapshots, records, changes)

**Files:**
- Create: `corpus_sync/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: `config.DB_PATH`.
- Produces (all take `conn: sqlite3.Connection`):
  - `store.open_db(path: Path | None = None) -> sqlite3.Connection` (creates `DATA_DIR/db`, `PRAGMA journal_mode=WAL`)
  - Schema (created in `open_db`):
    ```sql
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
    ```
  - `store.record_snapshot(conn, *, snap_id, created_at, status, total_docs, raw_path, book_counts: dict)`
  - `store.insert_records(conn, snap_id, rows: Iterable[dict])` (INSERT OR REPLACE, executemany)
  - `store.snapshot_status(conn, snap_id) -> str`
  - `store.load_record_hashes(conn, snap_id) -> dict[str, dict]` (source_id → {record_hash, arabic_hash, english_hash, metadata_hash, book})
  - `store.record_changes(conn, snap_id, rows: list[dict])`
  - `store.latest_snapshot(conn, *, complete_only: bool = True) -> str | None`

- [ ] **Step 1: Write failing tests** — `tests/test_store.py`: open tmp db; record 2 snapshots (1 complete, 1 partial); insert records; assert `latest_snapshot(complete_only=True)` skips partial; round-trip hashes; re-inserting same PK replaces (idempotent).
- [ ] **Step 2: Run to verify failure** — `PY -m pytest tests/test_store.py -v` → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/store.py`.
- [ ] **Step 4: Run to verify pass** — `PY -m pytest tests/test_store.py -v` → PASS.
- [ ] **Step 5: Commit** — `git add corpus_sync/store.py tests/ && git commit -m "feat(corpus-sync): sqlite store for snapshots/records/changes"`.

---

### Task 6: Sentence-level diff (Arabic + English, punctuation-invariant matching)

**Files:**
- Create: `corpus_sync/diff.py`
- Test: `tests/test_diff.py`

**Interfaces:**
- Consumes: `normalize.split_sentences`, `normalize.field_hash`.
- Produces:
  - `diff.sentence_diff(old_text: str, new_text: str, lang: str) -> SentenceDiff` — `@dataclass SentenceDiff(added: int, removed: int, modified: int)` where matching uses `difflib.SequenceMatcher(None, [field_hash(s) for s in old_sentences], [field_hash(s) for s in new_sentences])`; `modified` counts `replace`/`insert`+`delete` pair positions from opcodes (insert→added, delete→removed, replace→min(len) modified).

- [ ] **Step 1: Write failing tests**:

```python
def test_punctuation_only_change_is_not_a_change():
    d = diff.sentence_diff("Hello, world.", "Hello world.", "en")
    assert (d.added, d.removed, d.modified) == (0, 0, 0)

def test_added_and_removed_sentences():
    d = diff.sentence_diff("One. Two.", "One. Three. Four.", "en")
    assert (d.added, d.removed, d.modified) == (2, 1, 0)

def test_modified_sentence_detected():
    d = diff.sentence_diff("One. Two.", "One. Two changed.", "en")
    assert d.modified == 1 and d.added == 0

def test_arabic_reordering_not_flagged_as_modified():
    a = "أحمد بن محمد. قال الامام."
    b = "قال الامام. أحمد بن محمد."
    d = diff.sentence_diff(a, b, "ar")
    assert d.added == 0 and d.removed == 0 and d.modified == 0   # same set of sentences
```
- [ ] **Step 2: Run to verify failure** → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/diff.py`.
- [ ] **Step 4: Run to verify pass** → PASS (4 tests).
- [ ] **Step 5: Commit** — `git add corpus_sync/diff.py tests/ && git commit -m "feat(corpus-sync): sentence-level diff, punctuation-invariant"`.

---

### Task 7: Change detection (doc/field/sentence; deletion only on complete snapshots)

**Files:**
- Create: `corpus_sync/detect.py`
- Create: `tests/fixtures/e2e_snapshots/` — two REAL mini-snapshots (one small book, e.g. Faḍāʾil al-Shīʿa, 45 docs): `snap_A.jsonl`, `snap_B.jsonl` (B = A with 2 docs artificially re-fetched as-is + 1 doc's `raw.english` extended with one word + 1 doc removed), each with `meta.json` (book, fetched_at, sha256, counts) and a `fixture_manifest.json` entry explaining B is derived from A (so e2e tests are reproducible without network).
- Test: `tests/test_detect.py`

**Interfaces:**
- Consumes: `store`, `diff`, `normalize`.
- Produces:
  - `detect.detect_changes(conn, *, old_snap_id: str, new_snap_id: str) -> ChangeSet`
    - `@dataclass ChangeSet(new: list[str], modified: list[str], unchanged: int, removed: list[str], details: dict[str, dict])` — `details[source_id] = {"changed_fields": [...], "sentence_stats": {...}}` for MODIFIED docs.
    - REMOVED rows only if `store.snapshot_status(new) == 'complete'` AND `store.snapshot_status(old) == 'complete'`; otherwise raises `detect.IncompleteSnapshotError(old_snap_id, new_snap_id)` (after writing nothing).
    - Writes `store.record_changes(...)` rows (UNCHANGED rows are NOT stored — count only).
    - For MODIFIED docs: `changed_fields` = which of `[arabic, english, metadata]` hashes differ; `sentence_stats` computed via `diff.sentence_diff` on the raw field texts (loaded from the new/old raw JSONL paths — `detect` reads raw lines by `source_id` from both snapshots' `raw_path`).
  - `detect.load_raw_index(raw_path: Path) -> dict[str, dict]` (source_id → raw doc; lazy line scan).

- [ ] **Step 1: Write failing tests** — build a tmp store from the two e2e fixture snapshots (insert records via `normalize.normalize_doc`); assert:
  - `len(cs.new) == 0`, `len(cs.modified) == 1`, `len(cs.removed) == 1`, unchanged count == 43.
  - `cs.details[modified_id]["changed_fields"] == ["english"]`.
  - `cs.details[modified_id]["sentence_stats"]["en"]["modified"] >= 1` and `["ar"]["modified"] == 0`.
  - Partial new snapshot → `pytest.raises(IncompleteSnapshotError)`.

```python
def test_change_categories_on_real_fixture_pair(tmp_path): ...
def test_partial_snapshot_blocks_removed(tmp_path): ...
```
- [ ] **Step 2: Run to verify failure** → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/detect.py`.
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Commit** — `git add corpus_sync/detect.py tests/ && git commit -m "feat(corpus-sync): doc/field/sentence change detection"`.

---

### Task 8: Monthly analytics (14 metrics, exact formulas, report rendering)

**Files:**
- Create: `corpus_sync/analytics.py`
- Test: `tests/test_analytics.py`

**Interfaces:**
- Consumes: `store`.
- Produces:
  - `analytics.monthly_metrics(conn, *, month: str, new_snap_id: str, old_snap_id: str | None) -> dict` with exactly these keys (spec formulas):
    ```
    month, new_snapshot, old_snapshot,
    docs_total,                        # current complete snapshot doc count
    docs_new, docs_modified, docs_removed, docs_unchanged,
    sentences_new_ar, sentences_modified_ar, sentences_removed_ar,
    sentences_new_en, sentences_modified_en, sentences_removed_en,
    pct_docs_growth,                   # docs_new / old_total * 100  (0.0 if old is None)
    pct_text_change_ar,                # (sentences_new_ar + sentences_modified_ar) / current_ar_sentence_total * 100
    pct_text_change_en,                # same for en
    pct_docs_removed,                  # docs_removed / old_total * 100
    per_book: {book: {new, modified, removed}},
    vs_previous_month: dict | None,    # same metric keys, previous month (None if no data)
    vs_baseline: dict,                 # cumulative from first complete snapshot to new_snap
    history: list[{month, docs_new, docs_modified, docs_removed, docs_total}]
    ```
    - Sentence totals for the % denominators: counted from the *current* snapshot's raw texts via `normalize.split_sentences` (cached in a `sentence_totals` table added to the store schema: `(snapshot_id, lang, total)`; computed once per snapshot in `analytics.ensure_sentence_totals(conn, snap_id, raw_path)`).
  - `analytics.render_report(month: str, m: dict, *, baseline_month: str) -> str` — markdown with all 14 metrics, per-book table, and `vs_baseline`; no non-measured claims.
- [ ] **Step 1: Write failing tests** — synthetic store: 3 snapshots (M1 complete 100 docs, M2 complete 105 docs incl. 2 new/2 modified/1 removed, M3) with controlled hashes; assert every metric key and formula exactly (e.g. `pct_docs_growth == 2/100*100`); `render_report` contains the month and each metric value; `vs_previous_month is None` for the first month.
- [ ] **Step 2: Run to verify failure** → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/analytics.py` (+ `sentence_totals` table via a `store` schema bump in `open_db` — keep backward compatible with `CREATE TABLE IF NOT EXISTS`).
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Commit** — `git add corpus_sync/analytics.py corpus_sync/store.py tests/ && git commit -m "feat(corpus-sync): monthly analytics with spec formulas + report"`.

---

### Task 9: Hugging Face publication (dataset layout, manifest, private, token via env)

**Files:**
- Create: `corpus_sync/publish.py`
- Test: `tests/test_publish.py`

**Interfaces:**
- Consumes: `store`, `config.HF_REPO_ID`, `config.HF_IS_PRIVATE`.
- Produces:
  - `publish.build_dataset_dir(dest: Path, *, conn, new_snap_id: str, month: str) -> Path` — lays out (spec layout):
    ```
    dest/
      data/current/normalized.parquet      # full normalized records, current snapshot (pyarrow)
      data/current/normalized.jsonl
      data/raw/<snap_id>.jsonl             # copy of raw snapshot
      changes/<month>.jsonl                # one line per changed doc (NEW/MODIFIED/REMOVED + fields + sentence stats)
      reports/<month>.md                   # from analytics.render_report
      manifests/manifest.json              # {repo, month, snapshot, created_at, docs_total, per_book counts, sha256 of each data file, norm_version, license: "UNCONFIRMED - private"}
      README.md                            # dataset card: origin, fields (data dictionary), month, "private until data license confirmed"
    ```
  - `publish.upload_dataset(token: str | None = None) -> str` — `token = token or os.environ["HF_TOKEN"]` (raise `PublishError("HF_TOKEN not set")` if absent); `huggingface_hub.HfApi().upload_folder(repo_id=config.HF_REPO_ID, folder_path=staged, repo_type="dataset", private=config.HF_IS_PRIVATE, create_repo=True)`; returns repo URL. Token is never written to the manifest/README/logs (test asserts `"HF_TOKEN" not in manifest_text` and token value absent from all files).
- [ ] **Step 1: Write failing tests** — use `tmp_path`; monkeypatch `huggingface_hub.HfApi` with a fake capturing kwargs; assert: layout files exist; manifest sha256s match files; `private is True`; missing token raises; token never appears in any staged file (write a sentinel token).
- [ ] **Step 2: Run to verify failure** → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/publish.py`.
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Commit** — `git add corpus_sync/publish.py tests/ && git commit -m "feat(corpus-sync): HF dataset staging + private upload"`.

---

### Task 10: CLI + first real full snapshot (baseline)

**Files:**
- Create: `corpus_sync/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces (argparse subcommands, all exit 0 on success / 1 on failure, output UTF-8 files under `DATA_DIR/out/`):
  - `python -m corpus_sync.cli snapshot [--strict]` → run_snapshot (non-strict) → normalize+store into DB → write `out/snapshot-<id>.summary.json` (counts, per-book, incomplete list, duration, sha256).
  - `python -m corpus_sync.cli detect` → detect between latest two complete snapshots → `out/changes-<new_id>.json`.
  - `python -m corpus_sync.cli analyze --month YYYY-MM` → `out/report-YYYY-MM.md` + `.json`.
  - `python -m corpus_sync.cli publish --month YYYY-MM` → build + upload (HF_TOKEN from env).
  - `python -m corpus_sync.cli validate` → re-check latest snapshot completeness vs live `/v1/browse/books`; print pass/fail summary.
- [ ] **Step 1: Write failing tests** — `tests/test_cli.py`: run each subcommand against a tmp DATA_DIR seeded with the e2e fixture pair via `capsys`-free file assertions (CLI writes summaries); `publish` with fake HfApi.
- [ ] **Step 2: Run to verify failure** → FAIL.
- [ ] **Step 3: Implement** `corpus_sync/cli.py` (thin wiring over Tasks 3–9; single lock file `DATA_DIR/state/sync.lock` — if present and fresh (<24h), refuse to start (no concurrent syncs)).
- [ ] **Step 4: Run to verify pass** — full suite: `PY -m pytest tests/ -v` → all PASS.
- [ ] **Step 5: FIRST LIVE FULL SNAPSHOT** — run on the dev machine:
  `PY -m corpus_sync.cli snapshot` (expected ~1.5–2.5 h; checkpoint allows restart). Then `PY -m corpus_sync.cli validate`. Record results in `docs/corpus-sync/baseline-<date>.md`: duration, doc count (expected 50,038), per-book counts, incomplete list (must be empty), sha256. This is the baseline; it is REAL data (not test data).
- [ ] **Step 6: Commit** — `git add corpus_sync/cli.py tests/ docs/corpus-sync/baseline-*.md && git commit -m "feat(corpus-sync): cli + first live baseline snapshot"`.

---

### Task 11: GitHub Actions automation (weekly sync, monthly compare, publish, concurrency, artifacts)

**Files:**
- Create: `.github/workflows/corpus-sync.yml`
- Test: none (CI config); verified by pushing and watching one manual run.

**Interfaces:**
- Consumes: `corpus_sync` CLI, `HF_TOKEN` repo secret (user adds it to `saber13812002/rewayaat` → Settings → Secrets; never committed).
- Produces: workflow with:
  - `on:` schedule `cron: "0 3 * * 0"` (weekly, Sunday 03:00 UTC) + `workflow_dispatch` inputs: `action` (choices: `auto`, `snapshot`, `monthly`), `month` (string, for monthly).
  - `concurrency: {group: "corpus-sync", cancel-in-progress: false}` (no concurrent syncs across triggers).
  - Job `sync` (always): setup-python 3.13, `pip install -r corpus_sync/requirements.txt`, download artifact `corpus-state` (ignore if absent), run `snapshot` → `detect` (if previous complete snapshot exists in restored DB) → upload artifacts `corpus-state` (SQLite + state dir + latest raw pointer) and `corpus-raw` (raw JSONL).
  - Job `monthly` (needs sync; runs when `action=monthly` or day == 1): `analyze --month <current>` → upload artifact `corpus-report`.
  - Job `publish` (needs monthly; `if: github.event_name == 'workflow_dispatch' || github.run_id == needs.monthly.result...` — keep simple: runs after monthly; requires secret `HF_TOKEN`; on missing secret, `if` evaluates false and the job is skipped with a warning comment).
  - Failure handling: any job failure fails the run (visible on Actions tab + GitHub notifications to repo members); `sync` uploads state artifacts even on partial success (`if: always()` on the upload step) so the next run resumes from the checkpoint.
- [ ] **Step 1: Write the workflow file** per the above (single file; `actions/checkout@v4`, `actions/setup-python@v5`, `actions/download-artifact@v4`, `actions/upload-artifact@v4`).
- [ ] **Step 2: Validate YAML** — `PY -c "import yaml,sys; yaml.safe_load(open('.github/workflows/corpus-sync.yml'))"` (install `pyyaml` on the 3.13 interpreter: it has a wheel).
- [ ] **Step 3: Push branch and trigger** `workflow_dispatch` with `action=auto` from the fork's Actions tab (user clicks); watch `sync` complete a real snapshot run on the runner (log must show per-book counts and no secrets).
- [ ] **Step 4: Commit** — `git add .github/workflows/corpus-sync.yml && git commit -m "ci: weekly corpus sync + monthly compare + HF publish workflow"`.

---

### Task 12: Engineering delivery (data dictionary, recovery guide, pipeline README, final report A–J)

**Files:**
- Create: `docs/corpus-sync/pipeline-README.md` (how to run locally + via Actions, what each subcommand does, where data lives, env vars: only `HF_TOKEN`)
- Create: `docs/corpus-sync/data-dictionary.md` (every normalized field: name, type, source, transform, example; normalization rules v1.0; hash definitions; sentence-splitting rules per language)
- Create: `docs/corpus-sync/recovery-guide.md` (resuming a killed snapshot from checkpoint; re-running a failed book; DB rebuild from raw JSONL; what a partial snapshot means; how to add a new >10k book to the split plan; rate-limit/backoff tuning)
- Create: `docs/corpus-sync/report-2026-11.md` — the **final mission report**, sections A–J, each with status PASS/PARTIAL/FAIL and evidence links (files/commands/outputs):
  - A Executive Summary, B Source Discovery (→ `01-source-discovery.md`), C Extraction Report (baseline snapshot numbers), D Implementation (this plan + task commits), E Test Report (pytest summary counts), F HF Report (repo id, private, upload evidence — or PARTIAL if token not yet provided), G Automation (workflow file + first run evidence), H Monthly Analytics Sample (first report or PARTIAL with reason: needs two snapshots ≥1 month apart — first month report generated from baseline + a second same-day snapshot to exercise the pipeline, clearly labeled), I Repository Status (branch/commits/working tree state), J Remaining Work (data license decision, HF_TOKEN setup, ES option if ever needed, >10k book fallback smoke note from Task 3).
- [ ] **Step 1: Write the three docs** (data dictionary must match `normalize.py` field-for-field — diff-check against code).
- [ ] **Step 2: Generate the first analytics sample** — second snapshot (same day, `action=auto` local run) → `detect` → `analyze --month 2026-11` → embed the numbers (clearly labeled "baseline → same-day re-snapshot; expected ~0 changes except live edits").
- [ ] **Step 3: Write the final report A–J** with real evidence only; every status backed by a file path or command output.
- [ ] **Step 4: Run full test suite one last time** — `PY -m pytest tests/ -v` → all PASS; include the tail in report section E.
- [ ] **Step 5: Commit** — `git add docs/ && git commit -m "docs: data dictionary, recovery guide, pipeline README, final report"`.

---

## Self-Review Notes (run before execution)

- Spec coverage: Phase 2 → Tasks 3,10 (raw JSONL + hashes + baseline); Phase 3 → Task 4 (+Parquet/CSV in Task 9 `data/current` — CSV: add `current.jsonl`→CSV export in Task 9 if needed by spec; the spec's Parquet+CSV baseline is satisfied by `normalized.parquet` + `normalized.jsonl` (CSV of 50k wide rows is optional — if the user requires CSV, add `to_csv` in Task 9, one line, one test). Phase 4 → Tasks 3,10,11; Phase 5 → Tasks 6,7; Phase 6 → Task 8; Phase 7 → Task 9; Phase 8 → Task 11; Phase 9 → all `tests/` + Task 10 live run + e2e fixtures; Phase 10 → Task 12.
- Type consistency: `SnapshotResult`, `ChangeSet`, `SentenceDiff`, `LookupResult`, `Book`, `Query` — defined once (Tasks 2–7) and referenced by name thereafter.
- Proportion: plan carries the decisions (API behavior, limits, formulas, layout); implementers write bodies.
