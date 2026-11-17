#!/usr/bin/env bash
# Idempotent setup of the corpus-sync pipeline on server 133 (user: saber).
# No root/sudo required: clone into ~/rewayaat, user venv, user systemd units.
#
# Usage (on the server):  bash deploy/install-server.sh
#
# The hourly monitor timer is enabled immediately (monitoring must run
# from the first hour). The 6h sync timer is NOT auto-started: start it
# deliberately after the small-book smoke test passes (Phase 8):
#   systemctl --user start sync.timer
set -euo pipefail

REPO="https://github.com/saber13812002/rewayaat.git"
BRANCH="corpus-sync-pipeline"
ROOT="$HOME/rewayaat"

if [ ! -d "$ROOT/.git" ]; then
    git clone --branch "$BRANCH" "$REPO" "$ROOT"
else
    git -C "$ROOT" fetch origin "$BRANCH"
    git -C "$ROOT" checkout "$BRANCH"
    git -C "$ROOT" pull --ff-only origin "$BRANCH"
fi
cd "$ROOT"

if [ ! -x "$ROOT/.venv/bin/python" ]; then
    python3 -m venv .venv
fi
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r corpus_sync/requirements.txt

mkdir -p data/raw data/state data/out data/db logs monitoring/hourly

UNITDIR="$HOME/.config/systemd/user"
mkdir -p "$UNITDIR"
cp deploy/sync.service deploy/sync.timer \
   deploy/sync-monitor.service deploy/sync-monitor.timer "$UNITDIR"/
chmod +x deploy/sync_once.sh
systemctl --user daemon-reload
systemctl --user enable --now sync-monitor.timer
# sync.timer is left disabled on purpose (see header).

echo "== install complete =="
echo "repo      : $ROOT ($BRANCH @ $(git -C "$ROOT" rev-parse --short HEAD))"
echo "venv      : $ROOT/.venv"
echo "monitor   : systemctl --user status sync-monitor.timer"
echo "status    : $ROOT/.venv/bin/python monitor/linux_monitor.py status"
echo "hourly    : $ROOT/monitoring/hourly/ (newest = latest report)"
echo "journal   : journalctl --user -u sync.service -n 50"
echo "next step : systemctl --user start sync.timer   (after smoke test passes)"
