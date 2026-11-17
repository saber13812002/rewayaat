#!/usr/bin/env bash
# Resume-aware one-shot snapshot run (invoked by sync.service on server 133).
#
# Semantics (recovery-guide.md):
#   - state file present, no matching DONE line       -> crashed mid-run:
#     `snapshot --resume` continues where it stopped (completed pages are
#     skipped, uncompleted pages re-fetched).
#   - state file present, DONE line present BUT the last run recorded
#     failed queries (state["failed"] non-empty)       -> `snapshot --resume`
#     re-attempts ONLY the failed queries (idempotent: lookups dedupe at
#     finalize). This is how a transient API outage is healed on the next
#     timer tick instead of re-fetching the whole corpus from scratch.
#   - otherwise (clean DONE, or no state)              -> fresh full snapshot.
# The pipeline's own sync.lock guard blocks a concurrent run, so a timer
# tick that lands mid-run exits cleanly.
set -euo pipefail

ROOT="${REWAYAAT_ROOT:?REWAYAAT_ROOT must be set}"
cd "$ROOT"
VENV="$ROOT/.venv"
STATE="$ROOT/data/state/snapshot_state.json"
PLOG="$ROOT/data/state/snapshot_progress.log"

if [ -f "$STATE" ]; then
    decision="$("$VENV/bin/python" - "$STATE" "$PLOG" <<'PY'
import json, sys
state_path, plog = sys.argv[1], sys.argv[2]
try:
    st = json.load(open(state_path, encoding="utf-8"))
except Exception:
    print("resume")  # unreadable state: safer to let the CLI decide
    sys.exit(0)
snap = st.get("snapshot", "")
if not snap:
    print("fresh")
if st.get("failed"):
    # The last finalized run left failed queries: retry them, do not
    # re-fetch the corpus from scratch.
    print("resume")
    sys.exit(0)
try:
    with open(plog, encoding="utf-8") as fh:
        done = any(l.startswith("DONE dest=" + snap + " ") for l in fh)
except OSError:
    done = False
print("fresh" if done else "resume")
PY
)"
    if [ "$decision" = "resume" ]; then
        echo "sync_once: resuming snapshot (in-progress or has failed queries to retry)"
        exec "$VENV/bin/python" -m corpus_sync.cli snapshot --resume
    fi
fi
echo "sync_once: starting fresh full snapshot"
exec "$VENV/bin/python" -m corpus_sync.cli snapshot
