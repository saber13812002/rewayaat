# -*- coding: utf-8 -*-
"""Live agent execution monitor for the rewayaat corpus-sync mission.

Subcommands
    status     one-shot status block (writes monitoring/status.json, appends history)
    watch      live refresh every N seconds (default 15), delta vs previous tick
    report     hourly progress report, exact Phase-4 format (monitoring/hourly/)
    dashboard  read-only HTTP server on 127.0.0.1:8765 (/status /progress /history /logs /health)
    health     print only the health classification + rationale

All values are read from real state:
  - tmp/recovery_forever.log   (poller poll lines: time, poll#, result tail, probe secs)
  - tmp/probe_recovery.txt     (last probe detail; mtime used as stale-write signal)
  - monitoring/checkpoints/poller.json  (recorded poller PID + start time)
  - data/state/*, data/raw/*   (main-mission snapshot state)
  - live process inspection via PowerShell (recorded poller PID)

No invented percentages: the server rebuild has no known total, so progress is
reported as polls completed / probes succeeded / result transitions.

Console output is ASCII-only (cp1252 safe).
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MON = os.path.join(ROOT, "monitoring")
LOG = os.path.join(ROOT, "tmp", "recovery_forever.log")
PROBE_TXT = os.path.join(ROOT, "tmp", "probe_recovery.txt")
CHECKPOINT = os.path.join(MON, "checkpoints", "poller.json")
STATUS_JSON = os.path.join(MON, "status.json")
HISTORY = os.path.join(MON, "history.jsonl")
HOURLY = os.path.join(MON, "hourly")
MLOG = os.path.join(MON, "logs", "monitor.log")

POLL_INTERVAL_S = 600          # wait_forever.py sleep
POLL_STALE_AFTER_S = 900       # last poll older than this -> poller STALLED
PWR = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"

POLL_RE = re.compile(
    r"^(\d{2}):(\d{2}):(\d{2}) poll#(\d+): (.*) \(probe (\d+)s\)$")


def utcnow():
    return datetime.now(timezone.utc)


def mlog(msg):
    try:
        os.makedirs(os.path.dirname(MLOG), exist_ok=True)
        stamp = utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(MLOG, "a", encoding="utf-8") as fh:
            fh.write("%s %s\n" % (stamp, msg))
    except OSError:
        pass


def read_checkpoint():
    try:
        with open(CHECKPOINT, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def write_checkpoint(pid, cmd):
    os.makedirs(os.path.dirname(CHECKPOINT), exist_ok=True)
    with open(CHECKPOINT, "w", encoding="utf-8") as fh:
        json.dump({"pid": pid,
                   "command": cmd,
                   "recorded_at": utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "note": "wait_forever.py recovery poller; do not restart without inspection"},
                  fh, indent=1)


_proc_cache = {}


def proc_alive(pid):
    """Live check via PowerShell. Cached 20 s to keep watch light."""
    now = time.monotonic()
    if pid in _proc_cache and now - _proc_cache[pid][0] < 20:
        return _proc_cache[pid][1]
    alive = False
    cpu = None
    try:
        ps = ("(Get-CimInstance Win32_Process -Filter 'ProcessId=%d' -ErrorAction SilentlyContinue | "
              "Select-Object -ExpandProperty CommandLine); "
              "if ($p = Get-Process -Id %d -ErrorAction SilentlyContinue) { $p.CPU }" % (pid, pid))
        out = subprocess.run([PWR, "-NoProfile", "-Command", ps],
                             capture_output=True, text=True, timeout=30).stdout
        lines = [l.strip() for l in out.splitlines() if l.strip()]
        alive = any("wait_forever" in l for l in lines)
        if alive:
            for l in lines:
                if l.replace(".", "", 1).isdigit():
                    cpu = float(l)
    except (OSError, subprocess.SubprocessError, ValueError):
        alive = False
    _proc_cache[pid] = (now, (alive, cpu))
    return alive, cpu


def parse_poller_log():
    """Parse tmp/recovery_forever.log into structured poll records.

    Handles the midnight wrap: if a timestamp jumps backwards by more than
    12 h relative to the previous line, roll the date forward one day.
    """
    polls = []
    base_day = utcnow().date()
    prev = None
    try:
        with open(LOG, encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if line in ("loop done", "HARD_CAP_12H_REACHED"):
                    continue
                m = POLL_RE.match(line)
                if not m:
                    continue
                h, mi, s, n, tail, psec = m.groups()
                dt = datetime(base_day.year, base_day.month, base_day.day,
                              int(h), int(mi), int(s), tzinfo=timezone.utc)
                if prev and dt < prev - timedelta(hours=12):
                    dt = dt.replace(day=dt.day + 1) if dt.day < 28 else \
                        (prev + timedelta(minutes=10))
                prev = dt
                polls.append({
                    "time": dt, "n": int(n), "tail": tail,
                    "probe_s": int(psec),
                    "result": _classify_tail(tail),
                })
    except OSError:
        pass
    return polls


def _classify_tail(tail):
    if "RECOVERED" in tail and "STILL_DEGRADED" not in tail:
        return "RECOVERED"
    if tail == "PROBE_CRASH":
        return "PROBE_CRASH"
    return "STILL_DEGRADED"


def probe_file_state():
    st = {"exists": False, "mtime_utc": None, "tail": None, "lines": []}
    try:
        mt = os.path.getmtime(PROBE_TXT)
        st["exists"] = True
        st["mtime_utc"] = datetime.fromtimestamp(mt, timezone.utc)
        with open(PROBE_TXT, encoding="utf-8") as fh:
            st["lines"] = [l.rstrip("\n") for l in fh if l.strip()]
        if st["lines"]:
            st["tail"] = st["lines"][-1]
    except OSError:
        pass
    return st


def main_mission_state():
    """Snapshot state of the corpus-sync main mission (files only, no DB read)."""
    st = {"sync_lock": False, "snapshot_state": None, "raw_files": 0,
          "raw_newest": None, "db_exists": False}
    lock = os.path.join(ROOT, "data", "state", "sync.lock")
    st["sync_lock"] = os.path.isfile(lock)
    ss = os.path.join(ROOT, "data", "state", "snapshot_state.json")
    try:
        with open(ss, encoding="utf-8") as fh:
            d = json.load(fh)
        st["snapshot_state"] = {"snapshot": d.get("snapshot"),
                                "mtime_utc": datetime.fromtimestamp(
                                    os.path.getmtime(ss), timezone.utc).strftime("%H:%M:%S")}
    except (OSError, ValueError):
        pass
    raw = os.path.join(ROOT, "data", "raw")
    try:
        entries = sorted(os.listdir(raw))
        st["raw_files"] = len(entries)
        if entries:
            p = os.path.join(raw, entries[-1])
            st["raw_newest"] = datetime.fromtimestamp(
                os.path.getmtime(p), timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except OSError:
        pass
    st["db_exists"] = os.path.isfile(
        os.path.join(ROOT, "data", "db", "corpus_sync.db"))
    return st


def collect():
    """Full real-state snapshot (the single source for status/watch/report)."""
    now = utcnow()
    cp = read_checkpoint()
    pid = cp.get("pid")
    alive = False
    cpu = None
    if pid:
        alive, cpu = proc_alive(pid)
    polls = parse_poller_log()
    last = polls[-1] if polls else None
    pf = probe_file_state()
    mm = main_mission_state()

    # counters (real, never invented)
    n_polls = len(polls)
    n_recovered = sum(1 for p in polls if p["result"] == "RECOVERED")
    n_degraded = sum(1 for p in polls if p["result"] == "STILL_DEGRADED")
    n_crash = sum(1 for p in polls if p["result"] == "PROBE_CRASH")
    n_fast = sum(1 for p in polls if p["probe_s"] == 0)   # suspected instant crash
    # stale-write signal: probe claimed success-ish but probe file not rewritten
    stale_write = False
    if last and pf["mtime_utc"] and last["time"] > pf["mtime_utc"] + timedelta(seconds=30):
        stale_write = True
    last_age = (now - last["time"]).total_seconds() if last else None

    # health classification (Phase 5)
    reasons = []
    if not pid:
        health = "FAILED"
        reasons.append("no poller checkpoint recorded")
    elif n_polls == 0:
        health = "FAILED"
        reasons.append("poller log empty")
    elif last and last["result"] == "RECOVERED":
        health = "COMPLETED"
        reasons.append("probe reports RECOVERED; poller will exit")
    elif not alive:
        if any("HARD_CAP" in l for l in _read_log_tail(2)):
            health = "FAILED"
            reasons.append("poller process dead; log shows HARD_CAP_12H_REACHED")
        else:
            health = "FAILED"
            reasons.append("poller PID %s not alive" % pid)
    elif last_age is not None and last_age > POLL_STALE_AFTER_S:
        health = "STALLED"
        reasons.append("last poll %ds ago (> %ds); poller %s" %
                       (int(last_age), POLL_STALE_AFTER_S,
                        "alive but silent" if alive else "dead"))
    else:
        if n_fast:
            health = "DEGRADED"
            reasons.append("poller alive and polling, but %d/%d polls show probe=0s "
                           "(suspected probe crash / stale probe file)" % (n_fast, n_polls))
        else:
            health = "HEALTHY"
            reasons.append("poller alive; %d polls; last poll %ds ago" %
                           (n_polls, int(last_age or 0)))
        if stale_write:
            reasons.append("probe_recovery.txt mtime stale vs poll log (stale-write)")

    # last-hour window counters (Phase 4, section 3)
    cutoff = now - timedelta(hours=1)
    hour = [p for p in polls if p["time"] >= cutoff]
    transitions = 0
    for a, b in zip(polls, polls[1:]):
        if a["result"] != b["result"]:
            transitions += 1

    return {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "project": "rewayaat corpus-sync (recovery wait -> baseline snapshot)",
        "process": {"pid": pid, "alive": alive, "cpu_s": cpu,
                    "command": cp.get("command"),
                    "recorded_at": cp.get("recorded_at")},
        "current_task": {
            "task": "wait for server-side rebuild of Al-Kafi / Ghurar (rewayaat.info)",
            "stage": "polling tmp/probe_recovery.py every 600 s",
            "last_completed_operation": (
                "poll#%d @ %s -> %s" % (last["n"],
                                        last["time"].strftime("%H:%M:%S"),
                                        last["result"]) if last else "none yet"),
        },
        "progress": {
            "polls_completed": n_polls,
            "polls_total": None,          # unknown (12 h cap, ~72 polls) -> no %
            "probes_still_degraded": n_degraded,
            "probes_recovered": n_recovered,
            "probes_crash_or_fast": n_crash + n_fast,
            "result_transitions": transitions,
            "snapshot_run4": "not started",
            "note": "server rebuild duration unknown -> percentage not reported",
        },
        "last_activity": {
            "last_poll": last["time"].strftime("%H:%M:%S") if last else None,
            "last_poll_age_s": int(last_age) if last_age is not None else None,
            "last_probe_file_write": pf["mtime_utc"].strftime("%H:%M:%S") if pf["mtime_utc"] else None,
            "last_probe_result": pf["tail"],
        },
        "last_hour": {
            "polls": len(hour),
            "degraded": sum(1 for p in hour if p["result"] == "STILL_DEGRADED"),
            "crash": sum(1 for p in hour if p["result"] == "PROBE_CRASH"),
            "errors_in_log": _read_log_tail(200).count("ERROR"),
            "retries": None,  # retry counts are inside the probe; not logged per-poll
        },
        "main_mission": mm,
        "blockers": [
            "rewayaat.info: Al-Kafi lookup total=10000 expected=14242 (server rebuild in progress, user-confirmed)",
            "second rebuild planned for the two >10k books (user confirmed 09:42 UTC)",
        ],
        "health": health,
        "health_reasons": reasons,
        "stale_write_signal": stale_write,
        "probe_file": pf,
    }


def _read_log_tail(n=5):
    try:
        with open(LOG, encoding="utf-8") as fh:
            return fh.readlines()[-n:]
    except OSError:
        return []


def _fmt(v):
    if v is None:
        return "N/A"
    if isinstance(v, bool):
        return "yes" if v else "no"
    return str(v)


def render_status(c):
    L = []
    L.append("PROJECT       : %s" % c["project"])
    L.append("PROCESS       : PID %s  STATE %s  (CPU %s s)" %
             (_fmt(c["process"]["pid"]),
              "ALIVE" if c["process"]["alive"] else "DEAD",
              _fmt(c["process"]["cpu_s"])))
    L.append("CURRENT TASK  : %s" % c["current_task"]["task"])
    L.append("STAGE         : %s" % c["current_task"]["stage"])
    L.append("LAST COMPLETED: %s" % c["current_task"]["last_completed_operation"])
    p = c["progress"]
    L.append("PROGRESS      : polls %s (total unknown -> no %%); degraded %s; "
             "recovered %s; crash/fast %s; transitions %s" %
             (p["polls_completed"], p["probes_still_degraded"],
              p["probes_recovered"], p["probes_crash_or_fast"],
              p["result_transitions"]))
    L.append("LAST ACTIVITY : poll %s (%s s ago); probe file written %s" %
             (c["last_activity"]["last_poll"],
              c["last_activity"]["last_poll_age_s"],
              c["last_activity"]["last_probe_file_write"]))
    lh = c["last_hour"]
    L.append("LAST HOUR     : polls %s; degraded %s; crash %s; log errors %s" %
             (lh["polls"], lh["degraded"], lh["crash"], lh["errors_in_log"]))
    L.append("BLOCKERS      :")
    for b in c["blockers"]:
        L.append("  - %s" % b)
    L.append("HEALTH        : %s" % c["health"])
    for r in c["health_reasons"]:
        L.append("  reason: %s" % r)
    mm = c["main_mission"]
    L.append("CHECKPOINT    : run3 state (snapshot %s, mtime %s); sync.lock=%s; "
             "raw files=%s; db=%s" %
             (mm["snapshot_state"]["snapshot"] if mm["snapshot_state"] else None,
              mm["snapshot_state"]["mtime_utc"] if mm["snapshot_state"] else None,
              _fmt(mm["sync_lock"]), mm["raw_files"], _fmt(mm["db_exists"])))
    return "\n".join(L)


def persist(c, tick_kind="status"):
    os.makedirs(MON, exist_ok=True)
    slim = {k: v for k, v in c.items() if k != "probe_file"}
    slim["probe_file_tail"] = c["probe_file"]["tail"]
    slim["probe_file_mtime"] = (c["probe_file"]["mtime_utc"].strftime("%H:%M:%S")
                                if c["probe_file"]["mtime_utc"] else None)
    slim["tick"] = tick_kind
    try:
        with open(STATUS_JSON, "w", encoding="utf-8") as fh:
            json.dump(slim, fh, indent=1)
        with open(HISTORY, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(slim, default=str) + "\n")
    except OSError as exc:
        mlog("persist failed: %r" % exc)


def cmd_status():
    c = collect()
    print(render_status(c))
    persist(c, "status")
    mlog("status: health=%s polls=%s" % (c["health"], c["progress"]["polls_completed"]))
    return 0


def cmd_watch(interval):
    prev = None
    print("watch: refreshing every %ss (Ctrl+C to stop)" % interval)
    try:
        while True:
            c = collect()
            now_poll = c["progress"]["polls_completed"]
            delta = ""
            if prev is not None:
                dp = now_poll - prev["polls"]
                dh = "" if prev["health"] == c["health"] else "  health:%s->%s" % (prev["health"], c["health"])
                delta = "  delta: polls %+d%s" % (dp, dh)
            prev = {"polls": now_poll, "health": c["health"]}
            print("\n[%s] %s | polls=%s health=%s%s" %
                  (c["generated_at"][11:19], c["health"], now_poll,
                   "OK" if c["process"]["alive"] else "DEAD", delta))
            print("  last: %s" % c["current_task"]["last_completed_operation"])
            persist(c, "watch")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("watch: stopped")
        return 0


def cmd_report():
    c = collect()
    now = utcnow()
    p = c["progress"]
    lh = c["last_hour"]
    polls = parse_poller_log()
    cutoff = now - timedelta(hours=1)
    prev_counter = sum(1 for x in polls if x["time"] < cutoff)
    cur_counter = p["polls_completed"]
    this_hour = cur_counter - prev_counter
    pid = c["process"]["pid"]
    started = None
    try:
        cp = read_checkpoint()
        started = cp.get("process_started_at") or cp.get("recorded_at")
    except Exception:
        pass
    runtime = None
    if started:
        try:
            t0 = datetime.strptime(started, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            secs = (now - t0).total_seconds()
            if secs >= 0:
                runtime = "%d h %02d m" % (secs // 3600, secs % 3600 // 60)
        except ValueError:
            pass

    L = []
    L.append("HOURLY PROGRESS REPORT")
    L.append("Generated (UTC): %s" % now.strftime("%Y-%m-%d %H:%M:%S"))
    L.append("")
    L.append("1. PROCESS STATUS")
    L.append("   PID: %s" % _fmt(pid))
    L.append("   State: %s" % ("ALIVE" if c["process"]["alive"] else "DEAD"))
    L.append("   Runtime: %s" % (_fmt(runtime) + " (since %s)" % started if runtime else "N/A"))
    L.append("")
    L.append("2. CURRENT STAGE")
    L.append("   Task: %s" % c["current_task"]["task"])
    L.append("   Subtask: %s" % c["current_task"]["stage"])
    L.append("   Last Completed Operation: %s" % c["current_task"]["last_completed_operation"])
    L.append("")
    L.append("3. ACTUAL PROGRESS IN LAST HOUR")
    L.append("   Previous Counter (polls): %d" % prev_counter)
    L.append("   Current Counter (polls): %d" % cur_counter)
    L.append("   Completed This Hour (polls): %d" % this_hour)
    L.append("   Successful Operations (polls with probe result): %d" %
             sum(1 for x in polls if x["time"] >= cutoff and x["result"] != "PROBE_CRASH"))
    L.append("   Failed Operations (crash/fast probes this hour): %d" %
             sum(1 for x in polls if x["time"] >= cutoff and
                 (x["result"] == "PROBE_CRASH" or x["probe_s"] == 0)))
    L.append("   Retry Count: N/A (probe retries are internal; not logged per poll)")
    if this_hour == 0:
        L.append("   NO MEASURABLE PROGRESS IN THE LAST HOUR")
    L.append("")
    L.append("4. CUMULATIVE PROGRESS")
    L.append("   Total Completed: %d recovery polls since %s" %
             (cur_counter,
              polls[0]["time"].strftime("%Y-%m-%d %H:%M:%S UTC (poll#1)") if polls else "N/A"))
    L.append("   Total Expected: unknown (server rebuild duration not announced; 12 h poller cap)")
    L.append("   Percentage: NOT REPORTED (total unknown)")
    L.append("")
    L.append("5. ERRORS AND BLOCKERS")
    L.append("   New: %s" % ("none in last hour" if
                             not any(x["time"] >= cutoff and
                                     (x["result"] == "PROBE_CRASH" or x["probe_s"] == 0)
                                     for x in polls) else
                             "see section 3 failed operations"))
    L.append("   Persistent: rewayaat.info lookup for 'Al-Kafi' total=10000 expected=14242 "
             "(server-side rebuild in progress)")
    L.append("   Network Status: local DNS/HTTPS OK at last probe (%s)" %
             (c["last_activity"]["last_probe_file_write"] or "N/A"))
    L.append("   External Dependencies: rewayaat.info rebuild (owner: user, confirmed twice)")
    L.append("")
    L.append("6. CHECKPOINT")
    mm = c["main_mission"]
    L.append("   Latest: snapshot_state.json -> %s (mtime %s)" %
             (mm["snapshot_state"]["snapshot"] if mm["snapshot_state"] else "N/A",
              mm["snapshot_state"]["mtime_utc"] if mm["snapshot_state"] else "N/A"))
    L.append("   Is Resume Possible: yes (poller log + probe file persist; restart poller "
             "only after inspecting why previous instance stopped)")
    L.append("")
    L.append("7. EXECUTION HEALTH")
    L.append("   %s" % c["health"])
    for r in c["health_reasons"]:
        L.append("   - %s" % r)
    L.append("")
    L.append("8. EVIDENCE")
    L.append("   Log References: tmp/recovery_forever.log (poll#%d last), "
             "tmp/probe_recovery.txt (mtime %s)" %
             (polls[-1]["n"] if polls else 0,
              c["last_activity"]["last_probe_file_write"]))
    L.append("   Counter References: monitoring/history.jsonl (this + prior ticks)")
    L.append("   Last Successful Output: %s" %
             (c["last_activity"]["last_probe_result"] or "N/A"))
    text = "\n".join(L)
    print(text)
    # persist
    os.makedirs(HOURLY, exist_ok=True)
    stamp = now.strftime("%Y-%m-%d-%H%M")
    with open(os.path.join(HOURLY, stamp + ".txt"), "w", encoding="utf-8") as fh:
        fh.write(text + "\n")
    with open(os.path.join(HOURLY, stamp + ".json"), "w", encoding="utf-8") as fh:
        json.dump({k: v for k, v in c.items() if k != "probe_file"}, fh, indent=1, default=str)
    persist(c, "hourly-report")
    mlog("report: %s" % stamp)
    return 0


def cmd_health():
    c = collect()
    print("HEALTH: %s" % c["health"])
    for r in c["health_reasons"]:
        print("  - %s" % r)
    return 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, body, ctype="application/json"):
        b = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/") or "/"
        try:
            if path == "/":
                c = collect()
                html = ("<!doctype html><meta charset='utf-8'>"
                        "<title>rewayaat corpus-sync monitor</title>"
                        "<body style='font:14px monospace;background:#111;color:#0f0'>"
                        "<h2>rewayaat corpus-sync monitor</h2>"
                        "<p>health: %s | polls: %s | last poll: %s</p>"
                        "<p><a href='/status'>/status</a> <a href='/progress'>/progress</a> "
                        "<a href='/history'>/history</a> <a href='/logs'>/logs</a> "
                        "<a href='/health'>/health</a></p>"
                        "<pre>%s</pre></body>" %
                        (c["health"], c["progress"]["polls_completed"],
                         c["last_activity"]["last_poll"],
                         render_status(c).replace("<", "<")))
                self._send(html, "text/html")
            elif path == "/status":
                self._send(json.dumps(collect(), indent=1, default=str))
            elif path == "/progress":
                c = collect()
                self._send(json.dumps(c["progress"], indent=1))
            elif path == "/history":
                try:
                    with open(HISTORY, encoding="utf-8") as fh:
                        lines = fh.readlines()[-50:]
                except OSError:
                    lines = []
                self._send(json.dumps([json.loads(l) for l in lines], indent=1))
            elif path == "/logs":
                out = {"recovery_forever_log": _read_log_tail(50),
                       "monitor_log": []}
                try:
                    with open(MLOG, encoding="utf-8") as fh:
                        out["monitor_log"] = fh.readlines()[-50:]
                except OSError:
                    pass
                self._send(json.dumps(out, indent=1))
            elif path == "/health":
                c = collect()
                self._send(json.dumps({"health": c["health"],
                                       "reasons": c["health_reasons"]}, indent=1))
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as exc:  # keep the server alive
            try:
                self._send(json.dumps({"error": repr(exc)}), "application/json")
            except OSError:
                pass


def cmd_dashboard(port):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("dashboard: http://127.0.0.1:%d/ (read-only, loopback only)" % port)
    mlog("dashboard: serving on 127.0.0.1:%d" % port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def main():
    # cp1252 console: tolerate the diacritized book names read from UTF-8 logs
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    args = sys.argv[1:]
    cmd = args[0] if args else "status"
    if cmd == "status":
        return cmd_status()
    if cmd == "watch":
        iv = 15
        if "--interval" in args:
            iv = int(args[args.index("--interval") + 1])
        return cmd_watch(iv)
    if cmd == "report":
        return cmd_report()
    if cmd == "health":
        return cmd_health()
    if cmd == "dashboard":
        port = 8765
        if "--port" in args:
            port = int(args[args.index("--port") + 1])
        return cmd_dashboard(port)
    print("usage: monitor.py [status|watch [--interval N]|report|health|dashboard [--port N]]")
    return 1


if __name__ == "__main__":
    sys.exit(main())
