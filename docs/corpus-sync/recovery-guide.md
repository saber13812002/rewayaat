# Rewayaat Corpus-Sync — Recovery Guide

Runbook for the failure modes of the corpus sync pipeline. All commands run
from the repository root (Python 3.13, 64-bit). Code references point into
[`corpus_sync/`](../../corpus_sync/).

## Resuming a killed snapshot

A snapshot is checkpointed **after every page**: the state file
`data/state/snapshot_state.json` records which query pages completed, per-query
totals, repair progress, and the final sha256. The raw JSONL file
(`data/raw/snapshot-<ts>.jsonl`) is appended page by page (each append is
fsynced).

If a run dies mid-flight (crash, Ctrl-C, OOM, timeout), restart it with:

```bat
python -m corpus_sync.cli snapshot --resume
```

Rules implemented in [`_cmd_snapshot`](../../corpus_sync/cli.py) (L111–133):

1. **No state file** → error, nothing to resume.
2. **State file present** → the snapshot file named in it
   (`data/raw/<state.snapshot>`) is reused:
   - file exists → already-fetched pages are skipped; the run continues at
     the first uncompleted page. Retried pages are safe because lookup
     queries are idempotent and finalize dedupes by `source_id`
     ([`_dedupe`](../../corpus_sync/snapshot.py) L210–227, keep-last).
   - file missing and **no page had completed** → the file is recreated
     fresh (nothing was lost).
   - file missing but **some pages had completed** → the run **refuses**
     (exit 1) rather than silently re-fetching into a partial state.
     Fix: delete the orphan state file (`data/state/snapshot_state.json`)
     and start a fresh `snapshot` run.

Note: `--resume` requires that no *fresh* sync lock is present (see below).
Also note that the state file is **not deleted** after a successful run —
a subsequent plain `snapshot` run picks a new timestamped file name, which
name-mismatches the stale state and therefore starts fresh automatically.

### If the lock blocks you

A `snapshot` refuses to start while `data/state/sync.lock` is less than 24 h
old (a guard against concurrent runs). If the previous process is really
dead, either wait 24 h or delete the lock file:

```bat
del data\state\sync.lock
```

Check first that no Python process is running a snapshot:

```bat
tasklist | findstr python
```

## Re-running a failed book

If a single book's pages fail, the retry stack is:

- **HTTP layer** ([`http._request`](../../corpus_sync/http.py)): up to
  `MAX_RETRIES=4` attempts per request, exponential backoff 5 s → 40 s +
  jitter, on transport errors, 408, 429, and 5xx.
- **Page layer** ([`_lookup_with_retry`](../../corpus_sync/snapshot.py)
  L90–105): up to `SNAPSHOT_PAGE_RETRIES=3` attempts per *page*, backoff
  `15 s × attempt` (15 s, 30 s), when the whole request still fails.

Worst case per stuck page ≈ 3 page attempts × 4 request attempts × 240 s
timeout + backoffs — roughly an hour. If a page exhausts all retries, the
run raises and dies; **just re-run `snapshot --resume`** — completed pages
are skipped, and the failed page is simply retried (each attempt re-issues
the same idempotent query).

To retry a *single book only*, there is no per-book flag; the standard path
is `--resume` (fast if only one book remains) or a fresh full snapshot.

## What a "partial" snapshot means

If some book's fetched count drifts from its live `/v1/browse/books` count
(missing or extra documents), `verify_completeness`
([`snapshot.py`](../../corpus_sync/snapshot.py) L230–240) lists the affected
books and:

- the DB row is recorded with `status='partial'` (not `'complete'`);
- `--strict` makes the run exit 1 (`IncompleteSnapshotError`).

Downstream effects of a partial snapshot:

- `detect` and `publish` operate on **complete** snapshots only
  (`latest_snapshot(complete_only=True)`); a partial latest snapshot is
  skipped, and detection **refuses** to diff over a non-complete snapshot
  (`detect.IncompleteSnapshotError`).
- `analyze` likewise uses the latest *complete* snapshot.

So a partial snapshot never silently contaminates the change history — it
just waits. To recover: fix the underlying issue (usually a transient API
outage or a new book crossing the 10,000-doc threshold), re-run `snapshot`
(or `--resume`), and the new complete snapshot supersedes it.

The repair pass (snapshot.py L309–343) already covers the common case
automatically: for split books (≥10,000 docs) with a shortfall, it probes
individual `number:"n"` queries from the split fixture's seed list plus any
observed numbers, until the book's count matches.

## Rebuilding the database from raw JSONL

The SQLite store (`data/db/corpus_sync.db`) is fully derivable from the raw
snapshot files plus their summary JSON; it can be deleted and rebuilt at any
time. The stored facts per snapshot are: id (file-name stem), created_at,
status, total_docs, raw_path (relative to `data/`, POSIX), and per-book
counts — all present in `snapshot-*.jsonl` + `snapshot-*.summary.json`.

Minimal rebuild script (run from the repo root; adjust paths as needed):

```python
import json, datetime
from pathlib import Path
from corpus_sync import config, store, normalize

raw_dir = config.RAW_DIR
conn = store.open_db()  # creates data/db/corpus_sync.db if absent
for raw in sorted(raw_dir.glob("snapshot-*.jsonl")):
    summary = json.loads(raw.with_suffix(".summary.json").read_text(encoding="utf-8"))
    snap_id = "-".join(raw.stem.split("-")[1:])  # YYYYMMDD-HHMMSS-<seq>
    lines = [json.loads(l) for l in raw.open(encoding="utf-8") if l.strip()]
    status = "complete" if not summary["incomplete"] else "partial"
    store.record_snapshot(
        conn, snap_id=snap_id,
        created_at=summary["fetched_at"],
        status=status, total_docs=summary["docs"],
        raw_path=raw.relative_to(config.DATA_DIR).as_posix(),
        book_counts=summary["books"])
    rows = []
    for line in lines:
        rec = normalize.normalize_doc(line["raw"],
                                      book_slug=str(line.get("book", "")).lower(),
                                      fetched_at=line.get("fetched_at", snap_id))
        rows.append({k: rec[k] for k in ("source_id", "book", "chapter",
                    "hadith_number", "record_hash", "arabic_hash",
                    "english_hash", "metadata_hash", "fetched_at")})
    store.insert_records(conn, snap_id, rows)
    print(snap_id, len(rows))
conn.close()
```

This mirrors [`cli._store_records`](../../corpus_sync/cli.py) L59–73 exactly.
Change-history rows (`changes` table) and sentence totals are not rebuilt by
this script — they are regenerated by re-running `detect`/`analyze` over the
rebuilt snapshots (both are idempotent: `record_changes` uses
`INSERT OR REPLACE`, `ensure_sentence_totals` is idempotent).

## Adding a new >10,000-document book to the split plan

The LOOKUP API caps a single query at `max_result_window=10000`
(`MAX_PAGE_SIZE` in [`snapshot.py`](../../corpus_sync/snapshot.py) L108
area / `plan_queries` L118–151). Any book whose live count reaches
10,000 must be split into **per-chapter queries**, which requires an entry
in the `chapter_split.json` fixture (currently at
[`tests/fixtures/chapter_split.json`](../../tests/fixtures/chapter_split.json),
format: `{"books": {<book name>: {"chapters": [{"name": ...}]}}}`).

Procedure:

1. Confirm the book crossed the threshold:
   `python -m corpus_sync.cli validate` (FAIL lists drifting books) or
   inspect `data/out/snapshot-<id>.summary.json` → `incomplete`.
2. Fetch the book's chapter list (facets endpoint):
   ```python
   from corpus_sync import fetch
   print(fetch.facets("<Book Name>"))
   ```
3. Add an entry to `chapter_split.json` with every chapter name.
   Chapter names containing a double quote are handled: the query layer
   replaces `"` with `*` (a star-wildcard scope value, verified live in
   `01-source-discovery.md`).
4. Add a `numbers` seed list (as the existing Al-Kāfi / Ghurar entries have)
   so the repair pass can probe `number:"n"` queries for any shortfall.
5. Refresh the test fixture if your change affects the planned query shape
   (see `test_plan_*` in [`tests/test_snapshot.py`](../../tests/test_snapshot.py))
   and re-run `pytest tests/ -q`.
6. Re-run the snapshot; the book now appears as one query per chapter and
   should come back complete.

Current split books (verified against the live catalog, 2026-11-15):
**Al-Kāfi (14,242)** and **Ghurar al-Ḥikam (11,012)** — the only two books
above 10,000.

## Tuning rate limits and backoff

All tunables are constants in [`corpus_sync/config.py`](../../corpus_sync/config.py)
— no call-site changes needed:

| Constant | Default | Meaning |
|---|---|---|
| `PER_PAGE` | 200 | LOOKUP page size (250 API pages for the full corpus). |
| `REQUEST_TIMEOUT_S` | 240 | Per-request timeout; the live API answers the same query in 1.2 s…>120 s, so this is generous. |
| `MAX_RETRIES` | 4 | Attempts per request (1 + 3 retries) on transport/408/429/5xx. |
| `MIN_BACKOFF_S` | 5.0 | Exponential backoff base: 5 s, 10 s, 20 s, 40 s (+jitter). |
| `RATE_LIMIT_PER_S` | 2.0 | Sustained request rate (token bucket). The live API tolerates this comfortably. |
| `SNAPSHOT_PAGE_RETRIES` | 3 | Page-level attempts beyond the HTTP layer. |
| `SNAPSHOT_PAGE_BACKOFF_S` | 15.0 | Page-level backoff base: 15 s × attempt. |

Guidance:

- **Slow but healthy API** → leave as is; the worst-case budget is a full
  hour per stuck page (3 × 4 × 240 s + backoffs) and the workflow timeout
  is 480 min.
- **Server rejecting with 429/5xx under load** → lower `RATE_LIMIT_PER_S`
  (e.g. 1.0) and/or raise `MIN_BACKOFF_S`.
- **Server flapping briefly** → the current 3×4 attempt stack already
  absorbs outages of a few minutes per page; longer outages are handled by
  killing and re-running with `--resume` (no data is lost).

## Hugging Face publish failures

- **No token**: `publish` exits 1 without `HF_TOKEN`; the staged dataset is
  left under `data/out/publish/` for inspection. In CI the step prints a
  WARN and exits 0 instead (see workflow).
- **Upload error after staging**: re-run `publish --month YYYY-MM` —
  staging is rebuilt from the DB + raw files and the upload is idempotent
  (same commit path).
- **`raw_path` does not resolve**: the DB stores paths relative to `data/`;
  on a fresh machine restore both `data/db/` and `data/raw/` from CI
  artifacts before running `publish` (it re-normalizes from raw).
