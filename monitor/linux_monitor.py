#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Linux monitor for the rewayaat corpus-sync pipeline (server 133).

File-based and stdlib-only; safe to run repeatedly (read-only toward the
pipeline, except its own report files under monitoring/).

Subcommands:
  status   print + persist a status: process state, PID, stage, docs
           received, pages done, errors, retries, last real activity,
           last checkpoint, progress in the last hour.
  hourly   write an hourly report (text + JSON) under monitoring/hourly/
           and print the text. The systemd user timer runs this hourly.

Mission rules honored: hourly reporting, NO ETA is ever printed, and a
missing hour of progress is stated explicitly ("NO PROGRESS ...").

Paths default to the repo root (this file's grandparent directory);
override with the REWAYAAT_ROOT env var.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.environ.get("REWAYAAT_ROOT",
                           Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(ROOT))
try:
    from corpus_sync import config  # noqa: E402  (per_page must match the run)
    PER_PAGE = config.PER_PAGE
except Exception:  # pragma: no cover - monitor must not die on import drift
    PER_PAGE = 200

DATA = ROOT / "data"
STATE_FILE = DATA / "state" / "snapshot_state.json"
PROGRESS_LOG = DATA / "state" / "snapshot_progress.log"
LOCK_FILE = DATA / "state" / "sync.lock"
LAST_RUN = DATA / "out" / "last_run.log"
MON = ROOT / "monitoring"
HOUR = 3600

LINE_RE = re.compile(r"^(\S+)\s+(\w+)\s+(.*)$")
PAGE_RE = re.compile(r"label=(\S+) page=(\d+)/(\d+) docs=(\d+)")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(ts: str) -> datetime | None:
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def find_sync_pids() -> list[tuple[int, str]]:
    """(pid, cmdline) for live processes running the snapshot CLI."""
    out = []
    if not os.path.isdir("/proc"):
        return out
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as fh:
                cmd = fh.read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if "corpus_sync" in cmd and "snapshot" in cmd and "linux_monitor" not in cmd:
            out.append((int(entry), cmd.strip()))
    return out


def read_state() -> dict | None:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def iter_progress():
    """Yield (dt, level, msg) from the progress log, oldest first."""
    if not PROGRESS_LOG.is_file():
        return
    with open(PROGRESS_LOG, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            m = LINE_RE.match(line)
            if not m:
                continue
            dt = _parse_ts(m.group(1))
            if dt is None:
                continue
            yield dt, m.group(2), m.group(3)


def raw_doc_count(snapshot_name: str) -> int | None:
    p = DATA / "raw" / snapshot_name
    if not p.is_file():
        return None
    n = 0
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                n += 1
    return n


def planned_pages(state: dict) -> int:
    total = 0
    for t in (state.get("totals") or {}).values():
        total += (t + PER_PAGE - 1) // PER_PAGE if t else 0
    return total


def done_pages(state: dict) -> int:
    return sum(len(v) for v in (state.get("completed") or {}).values())


def build_report() -> dict:
    now = _utcnow()
    cutoff = now.timestamp() - HOUR
    state = read_state()
    pids = find_sync_pids()

    pages_h = 0
    docs_h = 0
    errors_h = 0
    warns_h = 0
    last_page = None
    last_error = None
    done_line = None
    last_line = None
    if state is not None:
        snap = state.get("snapshot", "")
        for dt, level, msg in iter_progress():
            if dt.timestamp() < cutoff:
                continue
            last_line = (dt, level, msg)
            if level == "PAGE":
                m = PAGE_RE.search(msg)
                if m:
                    pages_h += 1
                    docs_h += int(m.group(4))
                last_page = (dt, msg)
            elif level == "ERROR":
                errors_h += 1
                last_error = (dt, msg)
            elif level == "WARN":
                warns_h += 1
            elif level == "DONE" and snap and f"dest={snap}" in msg:
                done_line = (dt, msg)

    raw_docs = raw_doc_count(state.get("snapshot", "")) if state else None
    prev = previous_hourly()
    raw_prev = prev.get("raw_docs") if prev else None
    docs_since_prev = None
    if raw_docs is not None and isinstance(raw_prev, int):
        docs_since_prev = raw_docs - raw_prev

    # stage
    if state is None:
        stage = "idle (no snapshot state yet)"
    elif done_line is not None:
        stage = "finished (last run complete; summary written)"
    elif pages_h > 0:
        stage = "fetching"
    elif errors_h > 0:
        stage = "error-stalled (errors, no pages in the last hour)"
    else:
        stage = "NO PROGRESS in the last hour (no pages, no errors)"

    no_progress = state is not None and done_line is None and pages_h == 0

    lock_fresh = False
    if LOCK_FILE.is_file():
        age = time.time() - LOCK_FILE.stat().st_mtime
        lock_fresh = age < 24 * HOUR

    last_run = None
    try:
        last_run = json.loads(LAST_RUN.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        pass

    return {
        "at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot": state.get("snapshot") if state else None,
        "process": {
            "running": bool(pids),
            "pids": [p for p, _ in pids],
            "cmdline": [c for _, c in pids],
            "lock_present": LOCK_FILE.is_file(),
            "lock_fresh": lock_fresh,
        },
        "stage": stage,
        "no_progress_last_hour": no_progress,
        "docs_received_raw": raw_docs,
        "docs_in_last_hour": docs_h,
        "docs_since_previous_report": docs_since_prev,
        "pages_done": done_pages(state) if state else 0,
        "pages_planned": planned_pages(state) if state else None,
        "pages_in_last_hour": pages_h,
        "errors_last_hour": errors_h,
        "warnings_last_hour": warns_h,
        "failed_labels": sorted((state or {}).get("failed", [])),
        "last_real_activity": (state or {}).get("last_activity"),
        "last_page": f"{last_page[0].strftime('%Y-%m-%dT%H:%M:%SZ')} {last_page[1]}"
        if last_page else None,
        "last_error": f"{last_error[0].strftime('%Y-%m-%dT%H:%M:%SZ')} {last_error[1]}"
        if last_error else None,
        "last_checkpoint": (datetime.fromtimestamp(STATE_FILE.stat().st_mtime, tz=timezone.utc)
                            .strftime("%Y-%m-%dT%H:%M:%SZ")
                            if STATE_FILE.is_file() else None),
        "last_cli_run": last_run,
        # NOTE: no ETA by design (mission rule).
    }


def previous_hourly() -> dict | None:
    d = MON / "hourly"
    if not d.is_dir():
        return None
    js = sorted(d.glob("*.json"))
    if not js:
        return None
    try:
        return json.loads(js[-1].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def render_text(r: dict) -> str:
    p = r["process"]
    lines = [
        f"rewayaat corpus-sync status @ {r['at']}",
        f"  snapshot file : {r['snapshot'] or '-'}",
        f"  process       : {'RUNNING' if p['running'] else 'not running'}"
        + (f" (PID {', '.join(map(str, p['pids']))})" if p["running"] else ""),
        f"  sync lock     : {'present' if p['lock_present'] else 'absent'}"
        + (" (fresh)" if p["lock_fresh"] else ""),
        f"  stage         : {r['stage']}",
        f"  docs received : {r['docs_received_raw'] if r['docs_received_raw'] is not None else '-'}"
        f"  (last hour: +{r['docs_in_last_hour']},"
        f" since prev report: {r['docs_since_previous_report'] if r['docs_since_previous_report'] is not None else '-'})",
        f"  pages         : {r['pages_done']}/{r['pages_planned'] if r['pages_planned'] is not None else '?'}"
        f"  (last hour: +{r['pages_in_last_hour']})",
        f"  errors/retries: {r['errors_last_hour']} errors, {r['warnings_last_hour']} warnings (last hour)",
        f"  failed labels : {r['failed_labels'] or '[]'}",
        f"  last activity : {r['last_real_activity'] or '-'}",
        f"  last page     : {r['last_page'] or '-'}",
        f"  last error    : {r['last_error'] or '-'}",
        f"  last checkpoint: {r['last_checkpoint'] or '-'}",
    ]
    if r["no_progress_last_hour"]:
        lines.append("  *** NO PROGRESS in the last hour (no pages fetched, no errors) ***")
    if r["last_cli_run"]:
        lr = r["last_cli_run"]
        lines.append(f"  last CLI run  : {lr.get('started_at')} scope={lr.get('scope')} "
                     f"status={lr.get('status')} docs={lr.get('docs')} "
                     f"incomplete={lr.get('incomplete')} failed={lr.get('failed')}")
    lines.append("  (no ETA by design)")
    return "\n".join(lines)


def cmd_status() -> int:
    r = build_report()
    text = render_text(r)
    print(text)
    MON.mkdir(parents=True, exist_ok=True)
    (MON / "status.json").write_text(json.dumps(r, ensure_ascii=False, indent=1), encoding="utf-8")
    (MON / "status.txt").write_text(text + "\n", encoding="utf-8")
    return 0


def cmd_hourly() -> int:
    r = build_report()
    text = render_text(r)
    d = MON / "hourly"
    d.mkdir(parents=True, exist_ok=True)
    stamp = r["at"].replace("-", "").replace("T", "-").replace(":", "")
    (d / f"{stamp}.json").write_text(json.dumps(r, ensure_ascii=False, indent=1), encoding="utf-8")
    (d / f"{stamp}.txt").write_text(text + "\n", encoding="utf-8")
    # keep the last 14 days of reports
    old = sorted(d.glob("*.json"))[:-28]
    for p in old:
        try:
            p.unlink()
            if p.with_suffix(".txt").is_file():
                p.with_suffix(".txt").unlink()
        except OSError:
            pass
    print(text)
    return 0


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    if argv[0] == "status":
        return cmd_status()
    if argv[0] == "hourly":
        return cmd_hourly()
    print(f"unknown subcommand: {argv[0]} (use 'status' or 'hourly')", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
