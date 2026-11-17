#!/usr/bin/env bash
# Resume-aware one-shot snapshot run (invoked by sync.service on server 133).
#
# Semantics (recovery-guide.md):
#   - an in-progress snapshot (state file present, no matching DONE line in
#     the progress log) is resumed with `snapshot --resume`;
#   - otherwise a fresh full snapshot starts.
# The pipeline's own sync.lock guard blocks a concurrent run, so a timer
# tick that lands mid-run exits cleanly.
set -euo pipefail

ROOT="${REWAYAAT_ROOT:?REWAYAAT_ROOT must be set}"
cd "$ROOT"
VENV="$ROOT/.venv"
STATE="$ROOT/data/state/snapshot_state.json"
PLOG="$ROOT/data/state/snapshot_progress.log"

if [ -f "$STATE" ]; then
    snap="$("$VENV/bin/python" -c 'import json;print(json.load(open('"$STATE"')).get("snapshot",""))')"
    done_ok=0
    if [ -n "$snap" ] && [ -f "$PLOG" ] && grep -Fq "DONE dest=$snap " "$PLOG"; then
        done_ok=1
    fi
    if [ "$done_ok" -eq 0 ]; then
        echo "sync_once: resuming in-progress snapshot '$snap'"
        exec "$VENV/bin/python" -m corpus_sync.cli snapshot --resume
    fi
fi
echo "sync_once: starting fresh full snapshot"
exec "$VENV/bin/python" -m corpus_sync.cli snapshot
