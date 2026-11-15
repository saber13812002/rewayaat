# -*- coding: utf-8 -*-
"""Command-line interface for the corpus sync pipeline (T10).

Subcommands (all exit 0 on success, 1 on failure; artifacts are UTF-8
files under ``DATA_DIR/out/``):

- ``snapshot [--strict]``  fetch the full corpus into a new raw JSONL
  snapshot, normalize every doc into the store, and write
  ``out/snapshot-<id>.summary.json``.
- ``detect``               diff the latest two complete snapshots and
  write ``out/changes-<new_id>.json``.
- ``analyze --month YYYY-MM``  compute monthly metrics and write
  ``out/report-YYYY-MM.md`` + ``out/report-YYYY-MM.json``.
- ``publish --month YYYY-MM``  stage the dataset under
  ``out/publish/`` and upload it to Hugging Face (``HF_TOKEN`` env).
- ``validate``             re-check the latest snapshot's per-book counts
  against the live ``/v1/browse/books`` catalog.

Concurrent-run guard: a single lock file ``DATA_DIR/state/sync.lock``.
If it exists and was modified less than 24 h ago the command refuses to
start (no concurrent syncs); stale locks are removed and the command
proceeds.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import analytics, config, detect, normalize, publish, snapshot, store

LOCK_NAME = "sync.lock"
_LOCK_MAX_AGE_S = 24 * 3600


def _read_lines(path: Path) -> list[dict]:
    out = []
    if not path.is_file():
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=1) + "\n",
                    encoding="utf-8")


def _store_records(conn, snap_id: str, raw_path: Path) -> int:
    """Normalize every raw line of a snapshot into store records."""
    rows = []
    for line in _read_lines(raw_path):
        rec = normalize.normalize_doc(
            line["raw"],
            book_slug=str(line.get("book", "")).lower(),
            fetched_at=line.get("fetched_at", snap_id),
        )
        rows.append({k: rec[k] for k in (
            "source_id", "book", "chapter", "hadith_number",
            "record_hash", "arabic_hash", "english_hash",
            "metadata_hash", "fetched_at")})
    store.insert_records(conn, snap_id, rows)
    return len(rows)


def _acquire_lock() -> Path | None:
    """Return the lock path, or None if a fresh lock already exists."""
    lock = config.STATE_DIR / LOCK_NAME
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    if lock.exists():
        age = time.time() - lock.stat().st_mtime
        if age < _LOCK_MAX_AGE_S:
            return None
        lock.unlink()
    lock.write_text(
        json.dumps({"pid": None, "started_at":
                    datetime.now(timezone.utc).isoformat()}, ensure_ascii=False),
        encoding="utf-8")
    return lock


def _release_lock(lock: Path | None) -> None:
    if lock is not None:
        try:
            lock.unlink()
        except OSError:
            pass


# --- subcommands --------------------------------------------------------------


def _cmd_snapshot(args) -> int:
    lock = _acquire_lock()
    if lock is None:
        print(f"ERROR: fresh sync lock present ({LOCK_NAME}, <24 h old); "
              "another sync may be running.")
        return 1
    try:
        result = snapshot.run_snapshot(strict=args.strict)
        snap_id = result.path.stem  # snapshot-YYYYMMDD-HHMMSS-<seq>
        snap_id = "-".join(snap_id.split("-")[1:])  # YYYYMMDD-HHMMSS-<seq>
        conn = store.open_db()
        status = "complete" if not result.incomplete else "partial"
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        store.record_snapshot(
            conn, snap_id=snap_id, created_at=created_at, status=status,
            total_docs=result.docs, raw_path=str(result.path),
            book_counts=result.books)
        n_records = _store_records(conn, snap_id, result.path)

        summary_path = result.path.with_suffix(".summary.json")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["snapshot"] = snap_id
        summary["status"] = status
        summary["records_stored"] = n_records
        summary["database"] = str(config.DB_PATH)
        _write_json(config.OUT_DIR / f"snapshot-{result.path.stem}.summary.json",
                    summary)
        print(f"snapshot {snap_id}: docs={result.docs} status={status} "
              f"records={n_records} incomplete={result.incomplete or '[]'} "
              f"duration_s={result.duration_s} sha256={summary['sha256']}")
        return 0
    finally:
        _release_lock(lock)


def _cmd_detect(_args) -> int:
    conn = store.open_db()
    latest = store.latest_snapshot(conn)
    if latest is None:
        print("ERROR: no complete snapshot in the store; run `snapshot` first.")
        return 1
    previous = store.latest_snapshot_before(conn, latest)
    if previous is None:
        print("ERROR: need two complete snapshots to detect changes; "
              "only one is present.")
        return 1
    cs = detect.detect_changes(conn, old_snap_id=previous, new_snap_id=latest)
    out = {
        "new_snapshot": latest,
        "old_snapshot": previous,
        "new": sorted(cs.new),
        "modified": sorted(cs.modified),
        "removed": sorted(cs.removed),
        "unchanged": cs.unchanged,
        "details": {sid: cs.details[sid] for sid in sorted(cs.details)},
    }
    _write_json(config.OUT_DIR / f"changes-{latest}.json", out)
    print(f"changes {latest} vs {previous}: new={len(cs.new)} "
          f"modified={len(cs.modified)} removed={len(cs.removed)} "
          f"unchanged={cs.unchanged}")
    return 0


def _cmd_analyze(args) -> int:
    conn = store.open_db()
    latest = store.latest_snapshot(conn)
    if latest is None:
        print("ERROR: no complete snapshot in the store; run `snapshot` first.")
        return 1
    m = analytics.monthly_metrics(
        conn, month=args.month, new_snap_id=latest,
        old_snap_id=detect.previous_complete(conn, latest))
    _write_json(config.OUT_DIR / f"report-{args.month}.json", m)
    old_id = m["old_snapshot"]
    baseline_month = ("n/a (first snapshot)" if old_id is None
                      else f"{old_id[:4]}-{old_id[4:6]}")
    (config.OUT_DIR / f"report-{args.month}.md").write_text(
        analytics.render_report(args.month, m, baseline_month=baseline_month),
        encoding="utf-8")
    print(f"report {args.month}: docs_total={m['docs_total']} "
          f"new={m['docs_new']} modified={m['docs_modified']} "
          f"removed={m['docs_removed']}")
    return 0


def _cmd_publish(args) -> int:
    conn = store.open_db()
    latest = store.latest_snapshot(conn)
    if latest is None:
        print("ERROR: no complete snapshot in the store; run `snapshot` first.")
        return 1
    old_id = detect.previous_complete(conn, latest)
    if old_id is None:
        print("ERROR: publish requires a previous complete snapshot "
              "(for the change set); none found.")
        return 1
    staged = publish.build_dataset_dir(
        config.OUT_DIR / "publish", conn=conn,
        new_snap_id=latest, month=args.month)
    url = publish.upload_dataset(staged)
    print(f"published {latest} ({args.month}) -> {url}")
    return 0


def _cmd_validate(_args) -> int:
    conn = store.open_db()
    latest = store.latest_snapshot(conn)
    if latest is None:
        print("ERROR: no complete snapshot in the store; run `snapshot` first.")
        return 1
    row = conn.execute("SELECT book_counts FROM snapshots WHERE id=?",
                       (latest,)).fetchone()
    book_counts = json.loads(row[0])
    from . import fetch
    incomplete = snapshot.verify_completeness(book_counts, fetch.books())
    if incomplete:
        print(f"validate {latest}: FAIL — books with count drift vs live "
              f"catalog: {incomplete}")
        return 1
    print(f"validate {latest}: PASS — all {len(book_counts)} books match the "
          "live /v1/browse/books counts.")
    return 0


# --- argument parsing ---------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="corpus_sync.cli",
        description="Rewayaat corpus sync pipeline (snapshot/detect/analyze/"
                    "publish/validate).")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("snapshot", help="fetch the full corpus into a new "
                                         "snapshot and store it")
    sp.add_argument("--strict", action="store_true",
                    help="fail (exit 1) if any book is incomplete")
    sp.set_defaults(func=_cmd_snapshot)

    dp = sub.add_parser("detect", help="diff the latest two complete snapshots")
    dp.set_defaults(func=_cmd_detect)

    ap = sub.add_parser("analyze", help="compute monthly metrics")
    ap.add_argument("--month", required=True, metavar="YYYY-MM")
    ap.set_defaults(func=_cmd_analyze)

    pp = sub.add_parser("publish", help="stage + upload the HF dataset")
    pp.add_argument("--month", required=True, metavar="YYYY-MM")
    pp.set_defaults(func=_cmd_publish)

    vp = sub.add_parser("validate", help="re-check latest snapshot against "
                                         "the live book catalog")
    vp.set_defaults(func=_cmd_validate)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (snapshot.IncompleteSnapshotError,
            detect.IncompleteSnapshotError,
            publish.PublishError, OSError) as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
