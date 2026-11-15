# Rewayaat Corpus-Sync Pipeline — README

Continuous snapshotting, change detection, monthly analytics, and Hugging Face
publication for the Rewayaat hadith corpus (https://rewayaat.info), extracted
**API-only** via the site's REST endpoints (no Elasticsearch dependency).

Source discovery and API capabilities: [`01-source-discovery.md`](01-source-discovery.md).
Field-level documentation: [`data-dictionary.md`](data-dictionary.md).
Failure handling: [`recovery-guide.md`](recovery-guide.md).

## What the pipeline does

1. **snapshot** — fetches the entire corpus (~50,000 documents, 21 books)
   from the live REST API into a raw JSONL file, verifies per-book counts,
   and stores normalized records in a local SQLite database.
2. **detect** — diffs the latest two complete snapshots: new, modified,
   and removed hadith (sentence-level Arabic/English diff).
3. **analyze** — computes monthly change metrics and renders a Markdown report.
4. **publish** — stages a versioned dataset (normalized JSONL + Parquet +
   raw JSONL + changes + report + manifest) and uploads it to a private
   Hugging Face dataset.
5. **validate** — re-checks the latest snapshot's per-book counts against
   the live book catalog.

## Running locally

Requirements: Python 3.13 (64-bit).

```bat
python -m corpus_sync.cli --help
```

Install dependencies:

```bat
pip install -r corpus_sync/requirements.txt
```

> All commands run from the repository root; data paths are CWD-relative
> (see "Where data lives" below).

### Subcommands

| Command | What it does |
|---|---|
| `python -m corpus_sync.cli snapshot` | Fetch the full corpus into a new raw JSONL snapshot under `data/raw/`, record it in the DB, normalize all documents into the store, and write `data/out/snapshot-<id>.summary.json`. Prints a one-line summary (docs, status, sha256, duration). Takes ~30–90 min depending on live API latency. |
| `python -m corpus_sync.cli snapshot --strict` | Same, but exits 1 if any book's fetched count drifts from the live catalog count (incomplete snapshot). |
| `python -m corpus_sync.cli snapshot --resume` | Continue the in-progress snapshot named by `data/state/snapshot_state.json` instead of starting fresh. See [`recovery-guide.md`](recovery-guide.md#resuming-a-killed-snapshot) for the exact resume rules. |
| `python -m corpus_sync.cli detect` | Diff the latest two complete snapshots and write `data/out/changes-<new_id>.json` (new / modified / removed / unchanged + per-document changed fields and sentence stats). Exits 1 if fewer than two complete snapshots exist. |
| `python -m corpus_sync.cli analyze --month YYYY-MM` | Compute monthly metrics for the latest complete snapshot vs the previous complete snapshot; writes `data/out/report-YYYY-MM.md` and `.json`. With only one snapshot present, the baseline is `n/a (first snapshot)`. |
| `python -m corpus_sync.cli publish --month YYYY-MM` | Stage the dataset under `data/out/publish/` and upload it to Hugging Face using the `HF_TOKEN` environment variable. Requires two complete snapshots (for the change set) and an `HF_TOKEN`. |
| `python -m corpus_sync.cli validate` | Re-check the latest snapshot's stored per-book counts against the live `/v1/browse/books` catalog. Prints PASS/FAIL. |

All subcommands exit 0 on success and 1 on failure.

### Environment variables

The only environment variable the pipeline reads is:

| Variable | Used by | Notes |
|---|---|---|
| `HF_TOKEN` | `publish` only | Hugging Face read/write token for `saber13812002/rewayaat-corpus`. Not needed for snapshot/detect/analyze/validate. |

No API key is required for rewayaat.info (the REST API is public).

### Where data lives

All data is under `data/` (CWD-relative, gitignored):

```
data/
  db/corpus_sync.db          # SQLite store (WAL mode): snapshots, records,
                             #   changes, sentence_totals
  raw/snapshot-*.jsonl       # raw API documents, one JSON line per hadith
                             #   (source_id, book, fetched_at, page, label, raw)
  state/snapshot_state.json  # checkpoint: completed pages, totals, sha256
  state/sync.lock            # concurrent-run guard lock
  out/snapshot-*.summary.json# per-snapshot summary (counts, sha256, status)
  out/changes-*.json         # change-detection output
  out/report-YYYY-MM.md|.json# monthly analytics
  out/publish/               # staged HF dataset (normalized, raw, changes,
                             #   report, manifest, README)
```

The snapshot ID is the file name stem: `YYYYMMDD-HHMMSS-<seq>` (UTC).

### Concurrent-run guard

`data/state/sync.lock` prevents overlapping snapshot runs. If the lock file
exists and was modified **less than 24 h ago**, a new `snapshot` refuses to
start (exit 1); a stale lock (>= 24 h) is removed and the run proceeds. The
lock is released in a `finally` block on both success and failure.
`detect`/`analyze`/`publish`/`validate` do not take the lock.

## Running via GitHub Actions

Workflow: [`.github/workflows/corpus-sync.yml`](../../.github/workflows/corpus-sync.yml)
(`corpus-sync-pipeline` branch; merge to `master` to activate).

- **Schedule**: every Sunday 03:00 UTC (`cron: "0 3 * * 0"`).
- **Manual trigger**: Actions → Run workflow →
  `action` = `auto` (snapshot + detect + monthly compare + publish if
  token set), `snapshot` (fetch only), or `monthly` (compare + publish only
  over the latest stored state); optional `month` input overrides the
  computed month.
- **Concurrency**: group `corpus-sync`, `cancel-in-progress: false` — a new
  run waits for the in-flight one rather than cancelling it.

### Jobs

| Job | Steps | Artifacts uploaded |
|---|---|---|
| `sync` (timeout 480 min) | checkout → Python 3.13 → pip install → restore previous state + raw (skipped on first run) → clear stale machine lock → `snapshot` → `detect` (warns and continues on first run) | `corpus-state` (data/db + data/state), `corpus-raw` (data/raw) |
| `monthly` | runs on schedule, `auto`, or `monthly` dispatch: restore state + raw → `analyze --month <inputs.month or current UTC month>` | `corpus-report` (data/out) |
| `publish` | needs `monthly`: restore state + raw → `publish --month ...` using secret `HF_TOKEN` (warns and skips if the secret is not set) | `corpus-hf-staged` (data/out/publish) |

State continuity across weekly runs comes from the `corpus-state` and
`corpus-raw` artifacts: the DB stores `raw_path` **relative to `data/`**
(POSIX), and raw JSONL files are required because `publish` re-normalizes
from raw. Both artifacts are uploaded `if: always()` with
`if-no-files-found: ignore`, so even a failed run preserves its checkpoint.

Required repository secret: `HF_TOKEN` (optional — without it the dataset
is staged but not uploaded). The dataset is **private** until a data license
is confirmed.
