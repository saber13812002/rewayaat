# -*- coding: utf-8 -*-
"""Local watcher for the s52 snapshot extraction run.

Replaces the 12h-capped tmp/wait_forever.py poller. Polls the server-side
probe (ssh s52 "python3 /tmp/remote_status_probe.py") every POLL_S seconds,
appends to monitoring/history.jsonl, refreshes monitoring/status.json, writes
an hourly report to monitoring/hourly/<ts>.txt, and exits cleanly when the
snapshot reaches status=complete in the server SQLite (or the hard cap).

Derived states:
  RUNNING      process alive, DB not yet complete
  STALL        process alive but last_activity older than STALL_S
  CRASHED      no process, no new DB row (sync.timer will resume on next tick)
  FINALIZED    DB row present (status complete or partial)
  COMPLETE     DB row status=complete  -> done, exit 0
  PARTIAL      DB row status=partial   -> done for this run, exit 0
                 (sync_once.sh auto-retries failed queries at the next tick)

Usage:  python monitor/extraction_watcher.py [--once]
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HIST = ROOT / "monitoring" / "history.jsonl"
STATUS = ROOT / "monitoring" / "status.json"
HOURLY_DIR = ROOT / "monitoring" / "hourly"
DONE_FLAG = ROOT / "monitoring" / "extraction_done.json"

POLL_S = 900            # 15 min between probes
HARD_CAP_S = 96 * 3600  # 4 days
STALL_S = 3 * 3600      # no progress line for 3h while process alive
PROBE_TIMEOUT_S = 300

SSH = ["ssh", "s52", "python3 /tmp/remote_status_probe.py"]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_h(ts: str) -> str:
    return ts[:13] if ts and len(ts) >= 13 else ""


def probe() -> dict:
    try:
        p = subprocess.run(SSH, capture_output=True, text=True,
                           encoding="utf-8", errors="replace",
                           timeout=PROBE_TIMEOUT_S)
        line = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else ""
        return json.loads(line)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as e:
        return {"probe_error": repr(e)}


def derive(d: dict) -> tuple[str, list[str]]:
    """Return (state, warnings)."""
    warns: list[str] = []
    if "probe_error" in d:
        return "PROBE_ERROR", [f"probe failed: {d['probe_error']}"]

    proc = d.get("proc", [])
    db = (d.get("db") or {}).get("latest")
    state = d.get("state") or {}

    if db and db.get("status") == "complete":
        return "COMPLETE", warns
    if db and db.get("status") == "partial":
        warns.append(f"DB row partial; failed queries auto-retry on next "
                     f"sync.timer tick (every 6h)")
        return "PARTIAL", warns
    if not proc:
        if db:
            return "FINALIZED", warns
        warns.append("snapshot process not running and no DB row yet; "
                     "sync.timer (6h) will resume via sync_once.sh")
        return "CRASHED_OR_IDLE", warns
    # process alive
    last = state.get("last_activity") or ""
    if last:
        try:
            age = time.time() - datetime.strptime(
                last, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc).timestamp()
            if age > STALL_S:
                warns.append(f"process alive but no progress for {age/3600:.1f}h")
                return "STALL", warns
        except ValueError:
            pass
    return "RUNNING", warns


def rate_and_eta(d: dict, prev: dict | None) -> tuple[float, float | None]:
    """docs/hour rate and ETA hours based on live expected total (~50k)."""
    cur = (d.get("raw") or {}).get("lines", 0)
    if prev is None:
        return 0.0, None
    p = prev.get("_prev_raw") or 0
    if cur <= p:
        return 0.0, None
    elapsed_h = (time.time() - prev["_prev_t"]) / 3600
    if elapsed_h <= 0:
        return 0.0, None
    rate = (cur - p) / elapsed_h
    target = 50500  # live catalog total (~50038 + headroom)
    left = max(target - cur, 0)
    eta_h = left / rate if rate > 0 else None
    return rate, eta_h


def write_hourly(hour: str, d: dict, state: str, warns: list[str],
                 rate: float, eta: float | None) -> None:
    HOURLY_DIR.mkdir(parents=True, exist_ok=True)
    st = d.get("state") or {}
    raw = d.get("raw") or {}
    db = (d.get("db") or {}).get("latest") or {}
    lines = [
        f"hour={hour}  state={state}",
        f"snapshot: {st.get('snapshot')}  completed={st.get('completed')} "
        f"totals={st.get('totals')}  failed={len(st.get('failed', []))}",
        f"last_activity: {st.get('last_activity')}",
        f"raw: {raw.get('file')}  lines={raw.get('lines')}  "
        f"size_mb={raw.get('size_mb')}",
        f"db: {db or 'none yet (populated at finalize)'}",
        f"proc: {d.get('proc') or 'none'}",
        f"rate: {rate:.0f} docs/h  eta: "
        + (f"{eta:.1f}h" if eta is not None else "n/a"),
        f"warnings: {warns or 'none'}",
        f"failed labels: {st.get('failed', [])}",
    ]
    out_txt = HOURLY_DIR / f"{hour.replace(':', '')}.txt"
    out_json = HOURLY_DIR / f"{hour.replace(':', '')}.json"
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out_json.write_text(json.dumps({
        "hour": hour, "state": state, "raw": d.get("raw"),
        "state_file": {k: v for k, v in st.items() if k != "failed"},
        "failed": st.get("failed", []),
        "db": (d.get("db") or {}).get("latest"),
        "proc": d.get("proc"), "rate_docs_h": round(rate, 1),
        "eta_h": round(eta, 2) if eta is not None else None,
        "warnings": warns,
    }, ensure_ascii=False, indent=1), encoding="utf-8")


def append_history(entry: dict) -> None:
    HIST.parent.mkdir(parents=True, exist_ok=True)
    with open(HIST, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main() -> int:
    once = "--once" in sys.argv
    start = time.monotonic()
    prev: dict | None = None
    prev_hour = ""
    while True:
        d = probe()
        d["_poll"] = now_iso()
        d["_prev_raw"] = (prev or {}).get("_prev_raw", 0)
        d["_prev_t"] = (prev or {}).get("_prev_t", time.time())
        state, warns = derive(d)
        rate, eta = rate_and_eta(d, prev)
        hour = now_iso()[:13]  # YYYY-MM-DDTHH
        if hour != prev_hour:
            write_hourly(hour, d, state, warns, rate, eta)
            prev_hour = hour
        append_history({k: v for k, v in d.items() if not k.startswith("_")} |
                       {"derived_state": state, "warnings": warns,
                        "rate_docs_h": round(rate, 1),
                        "eta_h": round(eta, 2) if eta is not None else None})
        STATUS.parent.mkdir(parents=True, exist_ok=True)
        STATUS.write_text(json.dumps({
            "updated_at": now_iso(),
            "derived_state": state,
            "warnings": warns,
            "snapshot_state": d.get("state"),
            "failed_labels": (d.get("state") or {}).get("failed", []),
            "raw": d.get("raw"),
            "db_latest": (d.get("db") or {}).get("latest"),
            "proc": d.get("proc"),
            "rate_docs_h": round(rate, 1),
            "eta_h": round(eta, 2) if eta is not None else None,
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        tail = (d.get("state") or {}).get("last_activity", "")
        print(f"{now_iso()} [{state}] {tail} "
              f"completed={(d.get('state') or {}).get('completed')} "
              f"raw_lines={(d.get('raw') or {}).get('lines')} "
              f"rate={rate:.0f}/h eta={eta if eta is not None else '?'}h",
              flush=True)
        prev = d
        if state in ("COMPLETE", "PARTIAL"):
            DONE_FLAG.write_text(json.dumps(
                {"ts": now_iso(), "state": state, "probe": d},
                ensure_ascii=False, indent=1), encoding="utf-8")
            print(f"STOP: {state} recorded -> {DONE_FLAG.name}", flush=True)
            return 0
        if time.monotonic() - start > HARD_CAP_S:
            print("HARD_CAP_96H_REACHED", flush=True)
            return 2
        if once:
            return 0
        time.sleep(POLL_S)


if __name__ == "__main__":
    sys.exit(main())
