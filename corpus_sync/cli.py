# -*- coding: utf-8 -*-
"""Command-line interface for the corpus sync pipeline (T10).

Subcommands (all exit 0 on success, 1 on failure; artifacts are UTF-8
files under ``DATA_DIR/out/``):

- ``snapshot [--strict] [--resume]``  fetch the full corpus into a new raw
  JSONL snapshot, normalize every doc into the store, and write
  ``out/snapshot-<id>.summary.json``. ``--resume`` continues the in-progress
  snapshot named by ``DATA_DIR/state/snapshot_state.json`` instead of
  starting a fresh one (a crashed run can be picked up where it stopped).
- ``detect``               diff the latest two complete snapshots and
  write ``out/changes-<new_id>.json``.
- ``analyze --month YYYY-MM``  compute monthly metrics and write
  ``out/report-YYYY-MM.md`` + ``out/report-YYYY-MM.json``.
- ``publish --month YYYY-MM``  stage the dataset under
  ``out/publish/`` and upload it to Hugging Face (``HF_TOKEN`` env).
- ``validate``             re-check the latest snapshot's per-book counts
  against the live ``/v1/browse/books`` catalog.
- ``export [--raw P] [--snap ID] [--allow-partial] [--verify-live]
  [--dest D]``     build the standalone, integrity-hashed corpus export
  (records.jsonl + records.parquet + raw.jsonl + manifest.json +
  data_dictionary.md + mappings.json + README.md) under
  ``out/export/<snap_id>/``. Default source is the latest complete
  snapshot in the store; the export is ES-reimportable (see its README).
- ``load-es [--raw P] [--snap ID] [--embed] [--limit N] [--index I]``
  Phase 4: bulk-index export docs (+ enrichment + vectors) into
  Elasticsearch with doc id = source_id (idempotent re-runs).
- ``load-graph [--raw P] [--snap ID] [--limit N] [--related] [--rebuild]``
  Phase 4: load the Neo4j knowledge graph (UNWIND-MERGE, checkpointed);
  ``--related`` runs the deterministic RELATED_TO second pass.
- ``rag QUERY [--lang ar|en|fa|auto] [--book B] [--hadith N]
  [--volume V] [--top N] [--no-graph] [--index I]``
  Phase 4: hybrid ES retrieval (multi_match + kNN, RRF), 1-hop graph
  expansion, LLM answer with verified ⟦…⟧ citations + provenance log.
- ``graph-stats``  Phase 4: Neo4j label/relationship counts.
- ``es-stats [--index I]``  Phase 4: ES index count/shards/health.

Concurrent-run guard: a single lock file ``DATA_DIR/state/sync.lock``.
The lock stores the holder's pid. A fresh lock (<24 h old) held by a
*live* process blocks the command (no concurrent syncs); a lock left
behind by a dead holder (kill -9, OOM, power loss) or an old lock is
removed and the command proceeds, so a crash can never block the next
timer tick for 24 h.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import (analytics, config, detect, enrich, exporter, http, normalize,
               publish, raggraph, snapshot, store)

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


def _pid_alive(pid: int) -> bool:
    """Best-effort process liveness check.

    POSIX: os.kill(pid, 0). Other platforms: assume alive (safe default —
    the lock then expires by age alone).
    """
    if os.name != "posix":
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True      # exists, owned by another user
    except OSError:
        return True      # unknown failure: refuse to steal the lock
    return True


def _acquire_lock() -> Path | None:
    """Return the lock path, or None if a live sync already holds it."""
    lock = config.STATE_DIR / LOCK_NAME
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    if lock.exists():
        age = time.time() - lock.stat().st_mtime
        if age < _LOCK_MAX_AGE_S:
            # Fresh lock: only steal it if its recorded holder is dead.
            # Unreadable/unknown holders are treated as live (safe).
            try:
                holder = int(json.loads(lock.read_text(encoding="utf-8")).get("pid"))
            except (json.JSONDecodeError, OSError, TypeError, ValueError):
                holder = None
            if holder is None or _pid_alive(holder):
                return None
        lock.unlink()
    lock.write_text(
        json.dumps({"pid": os.getpid(), "started_at":
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


def _load_split() -> dict | None:
    """Load the committed chapter-split fixture (None if absent/corrupt).

    The fixture drives chapter-level queries for the books whose live count
    reaches LOOKUP's 10,000 max_result_window; without it those books are
    silently clamped at 10,000 docs (the Al-Kafi/Ghurar degradation).
    """
    if not config.SPLIT_FIXTURE.is_file():
        print(f"WARN: chapter split fixture missing ({config.SPLIT_FIXTURE}); "
              "books >= 10000 docs will be clamped at 10000.")
        return None
    try:
        return json.loads(config.SPLIT_FIXTURE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"WARN: chapter split fixture unreadable ({exc}); books "
              ">= 10000 docs will be clamped at 10000.")
        return None


def _cmd_snapshot(args) -> int:
    lock = _acquire_lock()
    if lock is None:
        print(f"ERROR: fresh sync lock present ({LOCK_NAME}, <24 h old, "
              "live holder); another sync may be running.")
        return 1
    try:
        config.OUT_DIR.mkdir(parents=True, exist_ok=True)
        run_log = config.OUT_DIR / "last_run.log"
        dest = None
        if getattr(args, "resume", False):
            state_path = config.STATE_DIR / snapshot.STATE_FILE
            if not state_path.is_file():
                print("ERROR: --resume given but no in-progress snapshot state "
                      f"file found ({state_path}).")
                return 1
            try:
                st = json.loads(state_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                print(f"ERROR: --resume state file is unreadable ({state_path}).")
                return 1
            dest = config.RAW_DIR / st.get("snapshot", "")
            if not dest.is_file():
                has_progress = bool(st.get("completed"))
                if has_progress:
                    print(f"ERROR: --resume state claims completed pages but the "
                          f"snapshot file is missing ({dest}); refusing to resume "
                          "silently (completed pages would be lost).")
                    return 1
                # No page had landed before the crash: safe to recreate the file.
                print(f"note: --resume snapshot file missing, none fetched; "
                      f"creating {dest.name} fresh.")
        if getattr(args, "books", None):
            book_list = [b.strip() for b in args.books.split(",") if b.strip()]
            if not book_list:
                print("ERROR: --books given but empty.")
                return 1
            # Scoped runs must not pollute the full-corpus summary baseline.
            books = book_list
        else:
            books = None
        result = snapshot.run_snapshot(dest=dest, strict=args.strict,
                                       split=_load_split(), books=books)
        status = "complete" if not result.incomplete else "partial"
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Store raw_path relative to DATA_DIR (POSIX) so the DB state is portable:
        # CI restores artifacts under a different absolute checkout, so an
        # absolute or CWD-relative path would not resolve on the next machine.
        try:
            raw_rel = result.path.relative_to(config.DATA_DIR).as_posix()
        except ValueError:
            raw_rel = str(result.path)
        # Scoped runs are test artifacts: do NOT record them in the store, or
        # they would masquerade as a complete corpus snapshot and break the
        # delete-only-on-verified-baseline rule for detect/publish.
        records_note = "n/a (scoped run, not stored)"
        snap_id = "-".join(result.path.stem.split("-")[1:])  # YYYYMMDD-HHMMSS-<seq>
        if books is None:
            conn = store.open_db()
            store.record_snapshot(
                conn, snap_id=snap_id, created_at=created_at, status=status,
                total_docs=result.docs, raw_path=raw_rel,
                book_counts=result.books)
            n_records = _store_records(conn, snap_id, result.path)
            records_note = str(n_records)
            conn.close()

        summary_path = result.path.with_suffix(".summary.json")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["snapshot"] = snap_id
        summary["status"] = status
        summary["records_stored"] = records_note
        summary["database"] = str(config.DB_PATH)
        summary["scoped_books"] = books
        # The raw file stem already starts with "snapshot-", so copying it
        # verbatim yields a clean "snapshot-<id>.summary.json" (the tests
        # glob snapshot-*.summary.json).
        _write_json(config.OUT_DIR / f"{result.path.stem}.summary.json",
                    summary)
        _write_json(run_log, {
            "started_at": created_at,
            "scope": books or "full",
            "status": status,
            "snapshot": snap_id,
            "docs": result.docs,
            "incomplete": result.incomplete,
            "failed": result.failed,
        })
        print(f"snapshot {snap_id}: docs={result.docs} status={status} "
              f"records={records_note} incomplete={result.incomplete or '[]'} "
              f"failed={result.failed or '[]'} "
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


def _cmd_export(args) -> int:
    conn = store.open_db()
    if args.raw:
        raw_path = Path(args.raw)
        if not raw_path.is_file():
            print(f"ERROR: --raw file not found: {raw_path}")
            return 1
        snap_id = args.snap or raw_path.stem
    else:
        snap_id = args.snap or store.latest_snapshot(
            conn, complete_only=not args.allow_partial)
        if snap_id is None:
            scope = "complete" if not args.allow_partial else ""
            print(f"ERROR: no {scope}snapshot in the store; run `snapshot` "
                  "first (or pass --raw).")
            return 1
        row = conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                           (snap_id,)).fetchone()
        if row is None:
            print(f"ERROR: snapshot {snap_id} not in the store "
                  "(scoped runs are not stored); pass --raw <path>.")
            return 1
        raw_path = store.resolve_raw_path(row[0])
    if not raw_path.is_file():
        print(f"ERROR: raw snapshot file not found: {raw_path}")
        return 1

    status = store.snapshot_status(conn, snap_id) or "unknown"
    verify_live = None
    if args.verify_live:
        from . import fetch
        verify_live = fetch.books()

    enr_path = Path(args.enrich) if args.enrich else None
    if enr_path is not None and not enr_path.is_file():
        print(f"ERROR: --enrich file not found: {enr_path}")
        return 1

    dest = Path(args.dest) if args.dest else config.OUT_DIR / "export" / snap_id
    manifest = exporter.build_export(
        dest, raw_path=raw_path, snap_id=snap_id, status=status,
        verify_live_books=verify_live, enrichment_path=enr_path)
    print(f"export {snap_id} (status={status}): {manifest['docs_total']} docs, "
          f"{len(manifest['books'])} books -> {dest}")
    cc = manifest.get("completeness_check")
    if cc is not None:
        if cc["incomplete"]:
            print(f"  WARNING: books incomplete vs live catalog: "
                  f"{cc['incomplete']}")
        else:
            print("  completeness vs live catalog: OK")
    if enr_path is not None:
        em = manifest.get("enrichment") or {}
        print(f"  enrichment merged from {enr_path.name}: "
              f"{em.get('enriched_docs')} docs")
    return 0


def _cmd_enrich(args) -> int:
    conn = store.open_db()
    if args.raw:
        raw_path = Path(args.raw)
        if not raw_path.is_file():
            print(f"ERROR: --raw file not found: {raw_path}")
            return 1
        snap_id = args.snap or raw_path.stem
    else:
        snap_id = args.snap or store.latest_snapshot(
            conn, complete_only=not args.allow_partial)
        if snap_id is None:
            scope = "complete" if not args.allow_partial else ""
            print(f"ERROR: no {scope}snapshot in the store; run `snapshot` "
                  "first (or pass --raw).")
            return 1
        row = conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                           (snap_id,)).fetchone()
        if row is None:
            print(f"ERROR: snapshot {snap_id} not in the store "
                  "(scoped runs are not stored); pass --raw <path>.")
            return 1
        raw_path = store.resolve_raw_path(row[0])
    if not raw_path.is_file():
        print(f"ERROR: raw snapshot file not found: {raw_path}")
        return 1

    client = enrich.LlmClient()
    books = [b.strip() for b in args.books.split(",") if b.strip()] \
        if args.books else None
    summary = enrich.run_enrichment(
        raw_path, snap_id, client=client, resume=not args.no_resume,
        limit=args.limit, books=books)
    print(f"enrich {snap_id}: {summary['enriched']}/{summary['total_docs']} "
          f"enriched, {summary['failed']} failed "
          f"({summary['processed_this_run']} this run) -> {summary['out_dir']}")
    if summary["failed"]:
        print(f"  WARNING: status={summary['status']}; failed ids: "
              f"{summary['failed_ids'][:10]}"
              + (" ..." if summary["failed"] > 10 else ""))
    if args.embed:
        esum = enrich.run_embeddings(raw_path, snap_id, client=client)
        print(f"  embeddings: {esum['vectors']} vectors "
              f"(model={esum['model']}) -> {esum['out_dir']}")
    return 0


def _resolve_raw(args):
    """Resolve (raw_path, snap_id) from --raw/--snap/--allow-partial.

    Prints an error and returns None when resolution fails.
    """
    conn = store.open_db()
    if args.raw:
        raw_path = Path(args.raw)
        if not raw_path.is_file():
            print(f"ERROR: --raw file not found: {raw_path}")
            return None
        snap_id = args.snap or raw_path.stem
    else:
        snap_id = args.snap or store.latest_snapshot(
            conn, complete_only=not args.allow_partial)
        if snap_id is None:
            scope = "complete" if not args.allow_partial else ""
            print(f"ERROR: no {scope}snapshot in the store; run `snapshot` "
                  "first (or pass --raw).")
            return None
        row = conn.execute("SELECT raw_path FROM snapshots WHERE id=?",
                           (snap_id,)).fetchone()
        if row is None:
            print(f"ERROR: snapshot {snap_id} not in the store "
                  "(scoped runs are not stored); pass --raw <path>.")
            return None
        raw_path = store.resolve_raw_path(row[0])
    if not raw_path.is_file():
        print(f"ERROR: raw snapshot file not found: {raw_path}")
        return None
    return raw_path, snap_id


def _cmd_load_es(args) -> int:
    res = _resolve_raw(args)
    if res is None:
        return 1
    raw_path, snap_id = res
    if args.embed:
        edir = config.OUT_DIR / "enrich" / snap_id
        if not (edir / enrich.EMBED_NAME).is_file():
            esum = enrich.run_embeddings(
                raw_path, snap_id, client=enrich.LlmClient())
            print(f"embeddings: {esum['vectors']} vectors "
                  f"(model={esum['model']}) -> {esum['out_dir']}")
    summary = raggraph.load_es(raw_path, snap_id, index=args.index,
                               limit=args.limit)
    print(f"load-es {snap_id}: {summary['indexed']}/{summary['total']} "
          f"docs -> {summary['index']} (errors={summary['errors']}, "
          f"no_vector={summary['no_vector']}, "
          f"enriched={summary['enriched']}, vectors={summary['vectors']})")
    return 0 if not summary["errors"] else 1


def _cmd_load_graph(args) -> int:
    res = _resolve_raw(args)
    if res is None:
        return 1
    raw_path, snap_id = res
    driver = raggraph.open_neo4j()
    try:
        enr_map = enrich.load_enrichment(
            config.OUT_DIR / "enrich" / snap_id / enrich.OUTPUT_NAME)
        summary = raggraph.load_graph(
            driver, raw_path, enr_map, snap_id=snap_id, limit=args.limit,
            rebuild=args.rebuild)
        print(f"load-graph {snap_id}: {summary['loaded']} loaded, "
              f"{summary['skipped_completed']} already done, "
              f"total {summary['total']} -> {summary['state']}")
        if args.related:
            rel = raggraph.run_related_pass(driver)
            print(f"related-pass: {rel['related_created']} RELATED_TO "
                  f"created over {rel['narrations']} narrations "
                  f"(top={rel['top']}, min_shared={rel['min_shared']})")
    finally:
        driver.close()
    return 0


def _cmd_rag(args) -> int:
    client = enrich.LlmClient()
    driver = None
    if not args.no_graph:
        try:
            driver = raggraph.open_neo4j()
        except Exception as exc:  # graph is an enhancement, not required
            print(f"WARNING: graph expansion disabled for this run: {exc}")
    try:
        result = raggraph.run_rag(
            args.query, lang=args.lang, book=args.book, hadith=args.hadith,
            volume=args.volume, top=args.top, use_graph=driver is not None,
            es=raggraph.EsClient(), driver=driver, client=client,
            index=args.index)
    finally:
        if driver is not None:
            driver.close()
    print(result["answer"] or "(no answer)")
    cit = result.get("citations")
    if cit:
        print(f"citations_verified: {cit['verified_count']}/{cit['total']} "
              f"({cit.get('status', 'n/a')})")
    print(f"provenance log: {result['provenance_log']}")
    return 0


def _cmd_graph_stats(_args) -> int:
    driver = raggraph.open_neo4j()
    try:
        stats = raggraph.graph_stats(driver)
    finally:
        driver.close()
    print("labels:")
    for lbl, c in stats["labels"].items():
        print(f"  {lbl}: {c}")
    print("relationships:")
    for rel, c in stats["relationships"].items():
        print(f"  {rel}: {c}")
    return 0


def _cmd_es_stats(args) -> int:
    stats = raggraph.es_stats(raggraph.EsClient(), index=args.index)
    print(f"{stats['index']}: count={stats['count']} "
          f"shards={stats['shards']} health={stats['health']}")
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
    sp.add_argument("--resume", action="store_true",
                    help="continue the in-progress snapshot named by "
                         "DATA_DIR/state/snapshot_state.json")
    sp.add_argument("--books", metavar="A,B,C",
                    help="only fetch these books (exact /v1/browse/books "
                         "names, comma-separated); scoped runs are NOT stored "
                         "in the DB (smoke tests / single-book re-runs)")
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

    xp = sub.add_parser("export",
                        help="standalone integrity-hashed corpus export "
                             "(JSONL+Parquet+manifest, ES-reimportable)")
    xp.add_argument("--raw", metavar="PATH",
                    help="raw snapshot JSONL to export (default: latest "
                         "complete snapshot in the store)")
    xp.add_argument("--snap", metavar="ID",
                    help="snapshot id (default: inferred from the --raw "
                         "filename, or the latest complete snapshot)")
    xp.add_argument("--allow-partial", action="store_true",
                    help="allow exporting the latest snapshot even if its "
                         "status is 'partial'")
    xp.add_argument("--verify-live", action="store_true",
                    help="query the live book catalog and record "
                         "completeness in the manifest (network call)")
    xp.add_argument("--dest", metavar="DIR",
                    help="output directory (default: "
                         "DATA_DIR/out/export/<snap_id>)")
    xp.add_argument("--enrich", metavar="PATH",
                    help="Phase-3 enrichment.jsonl to merge into the export "
                         "(fills `persian` + attaches `enrichment`)")
    xp.set_defaults(func=_cmd_export)

    ep = sub.add_parser("enrich",
                        help="Phase-3 LLM enrichment: trilingual + entities "
                             "/ topics / Quran & hadith references "
                             "(checkpointed, resumable)")
    ep.add_argument("--raw", metavar="PATH",
                    help="raw snapshot JSONL to enrich (default: latest "
                         "complete snapshot in the store)")
    ep.add_argument("--snap", metavar="ID",
                    help="snapshot id (default: inferred from the --raw "
                         "filename, or the latest complete snapshot)")
    ep.add_argument("--allow-partial", action="store_true",
                    help="allow enriching the latest snapshot even if its "
                         "status is 'partial'")
    ep.add_argument("--books", metavar="A,B,C",
                    help="only enrich these books (exact /v1/browse/books "
                         "names, comma-separated)")
    ep.add_argument("--limit", type=int, default=None, metavar="N",
                    help="enrich at most N pending docs this run")
    ep.add_argument("--no-resume", action="store_true",
                    help="start fresh (truncate enrichment.jsonl + state)")
    ep.add_argument("--embed", action="store_true",
                    help="after enriching, also generate embeddings "
                         "(arabic || english || persian) for enriched docs")
    ep.set_defaults(func=_cmd_enrich)

    lep = sub.add_parser(
        "load-es", help="Phase 4: index export docs into Elasticsearch "
                        "(doc id = source_id, idempotent)")
    lep.add_argument("--raw", metavar="PATH",
                     help="raw snapshot JSONL to index (default: latest "
                          "complete snapshot in the store)")
    lep.add_argument("--snap", metavar="ID",
                     help="snapshot id (default: inferred from the --raw "
                          "filename, or the latest complete snapshot)")
    lep.add_argument("--allow-partial", action="store_true",
                     help="allow the latest snapshot even if its status is "
                          "'partial'")
    lep.add_argument("--embed", action="store_true",
                     help="generate embeddings first if embeddings.jsonl "
                          "is missing")
    lep.add_argument("--limit", type=int, default=None, metavar="N",
                     help="index at most N docs")
    lep.add_argument("--index", metavar="NAME",
                     help="ES index name (default: config.ES_INDEX)")
    lep.set_defaults(func=_cmd_load_es)

    lgp = sub.add_parser(
        "load-graph", help="Phase 4: load the Neo4j knowledge graph "
                           "(checkpointed UNWIND-MERGE)")
    lgp.add_argument("--raw", metavar="PATH",
                     help="raw snapshot JSONL to load (default: latest "
                          "complete snapshot in the store)")
    lgp.add_argument("--snap", metavar="ID",
                     help="snapshot id (default: inferred from the --raw "
                          "filename, or the latest complete snapshot)")
    lgp.add_argument("--allow-partial", action="store_true",
                     help="allow the latest snapshot even if its status is "
                          "'partial'")
    lgp.add_argument("--limit", type=int, default=None, metavar="N",
                     help="load at most N docs")
    lgp.add_argument("--related", action="store_true",
                     help="run the deterministic RELATED_TO second pass "
                          "after loading")
    lgp.add_argument("--rebuild", action="store_true",
                     help="clear the whole graph first (MATCH DETACH DELETE)")
    lgp.set_defaults(func=_cmd_load_graph)

    rp = sub.add_parser(
        "rag", help="Phase 4: trilingual RAG query with verified citations")
    rp.add_argument("query", help="question (ar / en / fa)")
    rp.add_argument("--lang", choices=("ar", "en", "fa", "auto"),
                    default="auto", help="answer language (default: auto)")
    rp.add_argument("--book", metavar="NAME",
                    help="restrict to one book (exact name)")
    rp.add_argument("--hadith", metavar="N",
                    help="restrict to one hadith number")
    rp.add_argument("--volume", metavar="V",
                    help="restrict to one volume")
    rp.add_argument("--top", type=int, default=20, metavar="N",
                    help="top-N fused candidates (default: 20)")
    rp.add_argument("--no-graph", action="store_true",
                    help="skip Neo4j 1-hop expansion")
    rp.add_argument("--index", metavar="NAME",
                    help="ES index name (default: config.ES_INDEX)")
    rp.set_defaults(func=_cmd_rag)

    gps = sub.add_parser("graph-stats",
                         help="Phase 4: Neo4j label/relationship counts")
    gps.set_defaults(func=_cmd_graph_stats)

    eps = sub.add_parser("es-stats",
                         help="Phase 4: ES index count/shards/health")
    eps.add_argument("--index", metavar="NAME",
                     help="ES index name (default: config.ES_INDEX)")
    eps.set_defaults(func=_cmd_es_stats)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (snapshot.IncompleteSnapshotError,
            detect.IncompleteSnapshotError,
            publish.PublishError, exporter.ExportError, enrich.LlmError,
            http.HttpError, OSError, ValueError, RuntimeError) as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
