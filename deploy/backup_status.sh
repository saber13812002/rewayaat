#!/usr/bin/env bash
# Scheduled backup of critical rewayaat pipeline state (hourly, user timer).
#
# Backed up (rsync into ~/rewayaat/backups/<UTC-hourstamp>/):
#   - data/db/corpus_sync.db (+ WAL/SHM while open)      pipeline store
#   - data/raw/*.jsonl                                    raw snapshots
#   - data/state/                                         snapshot checkpoints
#   - data/out/enrich/<snap>/                             enrichment jsonl + state
#   - data/status/history.jsonl                           status API history
#   - ~/dedicated-rag/data/rewayaat/drag.db (+ WAL/SHM)   RAG vector store
#   - ~/dedicated-rag/data/rewayaat/index_*.log           index run logs
#
# Retention: last 72 hourly backups (3 days). Each run ends with a
# VERIFY line (tar -t test) so the log doubles as a backup-integrity audit.
# Restore test: see docs/corpus-sync/backup-restore-test.md
set -uo pipefail

ROOT="$HOME/rewayaat"
DRAG="$HOME/dedicated-rag/data/rewayaat"
STAMP="$(date -u +%Y%m%dT%H)"
DEST="$ROOT/backups/$STAMP"
mkdir -p "$DEST"

rsync -a --delete \
  "$ROOT/data/db" \
  "$ROOT/data/raw" \
  "$ROOT/data/state" \
  "$DEST/pipeline/" 2>&1 | sed 's/^/[rsync] /'

mkdir -p "$DEST/pipeline/out/enrich"
for d in "$ROOT"/data/out/enrich/*/; do
  [ -d "$d" ] || continue
  rsync -a "$d" "$DEST/pipeline/out/enrich/$(basename "$d")/" 2>&1 | sed 's/^/[rsync] /'
done

[ -f "$ROOT/data/status/history.jsonl" ] && cp "$ROOT/data/status/history.jsonl" "$DEST/pipeline/status_history.jsonl"

rsync -a "$DRAG/" "$DEST/drag/" --include='drag.db*' --include='index_*.log' --exclude='*' 2>&1 | sed 's/^/[rsync] /'

# integrity check on the two sqlite files (tar -t is a cheap read test;
# the real restore test is documented separately)
for f in "$DEST/pipeline/db/corpus_sync.db" "$DEST/drag/drag.db"; do
  if [ -f "$f" ]; then
    if command -v sqlite3 >/dev/null 2>&1; then
      rc=$(sqlite3 "file:$f?mode=ro" "PRAGMA integrity_check;" 2>&1)
      echo "[verify] $f integrity: $rc"
    else
      sz=$(stat -c%s "$f"); echo "[verify] $f size=$sz (sqlite3 not installed, size-only check)"
    fi
  fi
done

# retention: keep the 72 newest hour stamps
cd "$ROOT/backups" 2>/dev/null || exit 0
ls -1dt */ 2>/dev/null | tail -n +73 | while read -r old; do
  rm -rf "$ROOT/backups/${old%/}"
  echo "[retain] removed old backup ${old%/}"
done

echo "[backup] $STAMP complete -> $DEST"
du -sh "$DEST" 2>/dev/null
