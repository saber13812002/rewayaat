# -*- coding: utf-8 -*-
"""Hugging Face dataset staging + private upload (T9).

``build_dataset_dir`` lays out the spec dataset tree under ``dest``:

    dest/
      data/current/normalized.parquet   # full normalized records, current snap
      data/current/normalized.jsonl     # same records, one JSON per line
      data/raw/<snap_id>.jsonl          # byte-identical copy of the raw snapshot
      changes/<month>.jsonl             # one line per NEW/MODIFIED/REMOVED doc
      reports/<month>.md                # analytics.render_report output
      manifests/manifest.json           # repo/month/snapshot/counts/sha256s/license
      README.md                         # dataset card (data dictionary, license note)

The manifest carries the sha256 of every data file so a consumer can verify
integrity; the dataset stays ``private`` until a data license is confirmed
(``config.HF_IS_PRIVATE``). The HF token is passed to ``upload_folder`` but
never written to any staged file.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import analytics, config, store

# Normalized record columns in dataset order (the `raw` doc rides along as
# a nested JSON value so nothing is dropped from the normalized records).
_COLUMNS = (
    "source_id", "book", "chapter", "hadith_number", "narrator",
    "arabic", "english", "metadata", "fetched_at", "record_hash",
    "arabic_hash", "english_hash", "metadata_hash",
    "normalization_version", "raw",
)


class PublishError(RuntimeError):
    """Raised when the dataset cannot be staged or uploaded."""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _normalized_records(conn: sqlite3.Connection, snap_id: str) -> list[dict]:
    """One full normalized record per doc, in the v1.0 column order."""
    out: list[dict] = []
    for line in _raw_lines(conn, snap_id):
        rec = _normalize_line(line, snap_id)
        out.append({c: rec.get(c) for c in _COLUMNS})
    return out


def _raw_lines(conn: sqlite3.Connection, snap_id: str) -> list[dict]:
    row = conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                       (snap_id,)).fetchone()
    if row is None:
        raise PublishError(f"unknown snapshot: {snap_id!r}")
    lines = []
    with open(store.resolve_raw_path(row[0]), encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                lines.append(json.loads(line))
    return lines


def _normalize_line(line: dict, snap_id: str) -> dict:
    from . import normalize

    return normalize.normalize_doc(line["raw"],
                                   book_slug=line.get("book", "").lower(),
                                   fetched_at=line.get("fetched_at", snap_id))


def build_dataset_dir(dest: Path, *, conn: sqlite3.Connection,
                      new_snap_id: str, month: str) -> Path:
    """Stage the full dataset under ``dest``; returns ``dest``."""
    if dest.exists():
        shutil.rmtree(dest)
    (dest / "data" / "current").mkdir(parents=True)
    (dest / "data" / "raw").mkdir(parents=True)
    (dest / "changes").mkdir(parents=True)
    (dest / "reports").mkdir(parents=True)
    (dest / "manifests").mkdir(parents=True)

    row = conn.execute("SELECT total_docs, book_counts, created_at"
                       " FROM snapshots WHERE id=?", (new_snap_id,)).fetchone()
    if row is None:
        raise PublishError(f"unknown snapshot: {new_snap_id!r}")
    docs_total, book_counts, created_at = row[0], json.loads(row[1]), row[2]
    old_row = conn.execute(
        "SELECT id FROM snapshots WHERE status='complete' AND id<?"
        " ORDER BY id DESC LIMIT 1", (new_snap_id,)).fetchone()
    old_snap_id = old_row[0] if old_row else None

    # --- data/current: normalized records (jsonl + parquet) ----------------
    records = _normalized_records(conn, new_snap_id)
    norm_jsonl = dest / "data" / "current" / "normalized.jsonl"
    with open(norm_jsonl, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")

    # parquet: nested metadata/raw are stored as canonical JSON strings
    pq_records = []
    for r in records:
        r2 = dict(r)
        r2["metadata"] = json.dumps(r2["metadata"], ensure_ascii=False,
                                    sort_keys=True)
        r2["raw"] = json.dumps(r2["raw"], ensure_ascii=False)
        pq_records.append(r2)

    parquet = dest / "data" / "current" / "normalized.parquet"
    table = pa.Table.from_pylist(
        pq_records,
        schema=pa.schema([
            ("source_id", pa.string()),
            ("book", pa.string()),
            ("chapter", pa.string()),
            ("hadith_number", pa.string()),
            ("narrator", pa.string()),
            ("arabic", pa.string()),
            ("english", pa.string()),
            ("metadata", pa.string()),
            ("fetched_at", pa.string()),
            ("record_hash", pa.string()),
            ("arabic_hash", pa.string()),
            ("english_hash", pa.string()),
            ("metadata_hash", pa.string()),
            ("normalization_version", pa.string()),
            ("raw", pa.string()),
        ]))
    pq.write_table(table, parquet)

    # --- data/raw: byte-identical copy of the raw snapshot ------------------
    raw_row = conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                           (new_snap_id,)).fetchone()
    shutil.copyfile(store.resolve_raw_path(raw_row[0]), dest / "data" / "raw" / f"{new_snap_id}.jsonl")

    # --- changes: one line per NEW/MODIFIED/REMOVED doc ---------------------
    changes_jsonl = dest / "changes" / f"{month}.jsonl"
    with open(changes_jsonl, "w", encoding="utf-8") as fh:
        for sid, status, fields, stats in conn.execute(
                "SELECT source_id, doc_status, changed_fields, sentence_stats"
                " FROM changes WHERE snapshot_id=? ORDER BY source_id",
                (new_snap_id,)):
            fh.write(json.dumps({
                "source_id": sid,
                "doc_status": status,
                "changed_fields": (json.loads(fields) if fields else None),
                "sentence_stats": (json.loads(stats) if stats else None),
            }, ensure_ascii=False) + "\n")

    # --- report ---------------------------------------------------------------
    m = analytics.monthly_metrics(
        conn, month=month, new_snap_id=new_snap_id,
        old_snap_id=old_snap_id)
    baseline_month = ("n/a (first snapshot)" if old_snap_id is None
                      else f"{old_snap_id[:4]}-{old_snap_id[4:6]}")
    report_path = dest / "reports" / f"{month}.md"
    report_path.write_text(
        analytics.render_report(month, m, baseline_month=baseline_month),
        encoding="utf-8")

    # --- README (dataset card; written before the manifest so its sha256
    #     can be recorded there) ---------------------------------------------------
    readme_path = dest / "README.md"
    readme_path.write_text(_readme(month, new_snap_id, docs_total,
                                   book_counts, created_at),
                           encoding="utf-8")

    # --- manifest (sha256 over every staged file but itself) -----------------
    manifest_path = dest / "manifests" / "manifest.json"
    staged_files = {
        "data/current/normalized.parquet": parquet,
        "data/current/normalized.jsonl": norm_jsonl,
        f"data/raw/{new_snap_id}.jsonl": dest / "data" / "raw"
                                         / f"{new_snap_id}.jsonl",
        f"changes/{month}.jsonl": changes_jsonl,
        f"reports/{month}.md": report_path,
        "README.md": readme_path,
    }
    manifest = {
        "repo": config.HF_REPO_ID,
        "month": month,
        "snapshot": new_snap_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "docs_total": docs_total,
        "per_book": {b: c for b, c in sorted(book_counts.items())},
        "sha256": {rel: _sha256(p) for rel, p in sorted(staged_files.items())},
        "norm_version": config.NORM_VERSION,
        "license": config.HF_LICENSE,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")
    return dest


def _readme(month: str, snap_id: str, docs_total: int, book_counts: dict,
            created_at: str) -> str:
    books = "\n".join(f"| {b} | {c} |" for b, c in sorted(book_counts.items()))
    return f"""# Rewayaat corpus (monthly snapshot)

Versioned export of the [rewayaat.info](https://rewayaat.info) Shia hadith
corpus, produced by the `corpus_sync` pipeline. **Private until a data
license is confirmed** (manifest license: {config.HF_LICENSE}).

## Contents

| File | Description |
|---|---|
| `data/current/normalized.parquet` | Full normalized records, current snapshot |
| `data/current/normalized.jsonl` | Same records, JSON Lines |
| `data/raw/{snap_id}.jsonl` | Raw API documents (byte-identical snapshot copy) |
| `changes/{month}.jsonl` | Per-doc change detection results (NEW/MODIFIED/REMOVED) |
| `reports/{month}.md` | Monthly change report (spec formulas) |
| `manifests/manifest.json` | Repo metadata, per-book counts, file sha256 |

## Fields (data dictionary, normalization v{config.NORM_VERSION})

| Column | Description |
|---|---|
| `source_id` | Stable doc id from the API (`{{book}}:{{number}}`) |
| `book` / `chapter` / `hadith_number` | Bibliographic position |
| `narrator` | First narrator in the chain, if present |
| `arabic` / `english` | Full text, NFKC-normalized |
| `metadata` | All other raw fields as canonical JSON |
| `fetched_at` | ISO timestamp of the fetch |
| `record_hash` | sha256 over the canonical content (change detection key) |
| `arabic_hash` / `english_hash` / `metadata_hash` | Per-field NFKC hashes |
| `normalization_version` | Rule version (currently `{config.NORM_VERSION}`) |
| `raw` | Full raw API document |

## Current snapshot

- Snapshot: `{snap_id}` ({month}, created {created_at})
- Docs: {docs_total}

| Book | Docs |
|---|---|
{books}

## Integrity

Every staged file's sha256 is recorded in `manifests/manifest.json`; verify
downloads against it.
"""


def upload_dataset(staged: Path, token: str | None = None) -> str:
    """Upload the staged dataset to HF; returns the repo URL.

    Token resolution: explicit argument > ``HF_TOKEN`` env var >
    ``PublishError``. The token is passed to ``upload_folder`` but is never
    written into the manifest, README, or logs.
    """
    token = token or os.environ.get("HF_TOKEN")
    if not token:
        raise PublishError("HF_TOKEN not set")
    import huggingface_hub  # lazy: keep staging importable without the dep

    api = huggingface_hub.HfApi(token=token)
    api.upload_folder(
        repo_id=config.HF_REPO_ID,
        folder_path=staged,
        repo_type="dataset",
        private=config.HF_IS_PRIVATE,
        create_repo=True,
    )
    return f"https://huggingface.co/datasets/{config.HF_REPO_ID}"
