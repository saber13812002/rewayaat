#!/usr/bin/env bash
# Deploy the rewayaat drag provider on s52 (idempotent).
# Steps: tokens -> convert -> compose up -> init-db -> ingest (foreground)
#        -> index (background, resume-safe).
set -euo pipefail
RAG=/home/saber/dedicated-rag
REW=/home/saber/rewayaat
SNAP=snapshot-20261117-204731-1
RAW="$REW/data/raw/$SNAP.jsonl"
ENR="$REW/data/out/enrich/$SNAP/enrichment.jsonl"
PY="$REW/.venv/bin/python3"
cd "$RAG"

echo "== [1/7] preflight =="
id
wc -l "$RAW"
[ -f "$ENR" ] && wc -l "$ENR" || echo "no enrichment yet (persian blocks = 0)"
docker image inspect dedicated-rag:0.1.1 >/dev/null 2>&1 || { echo "FATAL: image dedicated-rag:0.1.1 missing"; exit 1; }
[ -f compose.rewayaat.yml ] || { echo "FATAL: compose.rewayaat.yml missing"; exit 1; }
df -h /home/saber | tail -1

echo "== [2/7] tokens =="
# normalize CRLF (files may be scp'd from Windows)
sed -i 's/\r$//' env/rewayaat.env shim/shim-rewayaat.env 2>/dev/null || true
if [ -f env/rewayaat.env ] && grep -qE '^DRAG_SERVICE_TOKEN=[0-9a-f]{64}$' env/rewayaat.env; then
  TOKEN=$(sed -n 's/^DRAG_SERVICE_TOKEN=//p' env/rewayaat.env)
  echo "reusing existing DRAG_SERVICE_TOKEN"
else
  TOKEN=$(openssl rand -hex 32)
  sed "s/__TOKEN__/${TOKEN}/" env/rewayaat.env > env/rewayaat.env.new
  mv env/rewayaat.env.new env/rewayaat.env
  echo "generated new DRAG_SERVICE_TOKEN"
fi
chmod 600 env/rewayaat.env
if ! grep -qE '^SHIM_UPSTREAM_TOKEN=[0-9a-f]{64}$' shim/shim-rewayaat.env 2>/dev/null; then
  SVC=$(openssl rand -hex 12)
  sed -e "s/__UPSTREAM__/${TOKEN}/" -e "s/__SERVICE__/${SVC}/" \
      shim/shim-rewayaat.env > shim/shim-rewayaat.env.new
  mv shim/shim-rewayaat.env.new shim/shim-rewayaat.env
  chmod 600 shim/shim-rewayaat.env
  echo "rendered shim env (SHIM_SERVICE_TOKEN=$SVC)"
else
  # keep upstream token in sync with provider token
  sed -i "s/^SHIM_UPSTREAM_TOKEN=.*/SHIM_UPSTREAM_TOKEN=${TOKEN}/" shim/shim-rewayaat.env
fi
echo "DRAG_SERVICE_TOKEN=${TOKEN}"

echo "== [3/7] convert (raw + enrichment -> blocks + manifest) =="
mkdir -p data/rewayaat
$PY data/rewayaat/convert_rewayaat.py --raw "$RAW" --enrich "$ENR" \
    --data-dir "$PWD/data/rewayaat"

echo "== [4/7] start provider + shim =="
docker compose -f compose.rewayaat.yml up -d
docker compose -f compose.rewayaat-shim.yml up -d
sleep 6
curl -sf http://127.0.0.1:8107/health && echo " <- provider /health" || echo "WARN: provider /health not up yet"
curl -sf http://127.0.0.1:8130/health && echo " <- shim /health" || echo "WARN: shim /health not up yet"

echo "== [5/7] init-db =="
docker run --rm --user 1001:1001 -v "$PWD/data/rewayaat:/data" --env-file ./env/rewayaat.env \
  dedicated-rag:0.1.1 python -m drag.cli init-db

echo "== [6/7] ingest-manifest (foreground; ~35k documents, 20-40 min) =="
: > data/rewayaat/ingest.log
docker run --rm --user 1001:1001 -v "$PWD/data/rewayaat:/data" --env-file ./env/rewayaat.env \
  dedicated-rag:0.1.1 python -m drag.cli ingest-manifest --manifest /data/rewaat_manifest.json \
  >> data/rewayaat/ingest.log 2>&1
echo "ingest tail:"; tail -2 data/rewayaat/ingest.log

echo "== [7/7] index (background, resume-safe) =="
if pgrep -f "drag.cli index --collection-id rewaat" >/dev/null 2>&1; then
  echo "index already running"
elif [ -f data/rewayaat/index.log ] && grep -q '"ok": true, "status": "indexed"' data/rewayaat/index.log; then
  echo "already indexed (index.log has final line)"
else
  : > data/rewayaat/index.log
  nohup docker run --rm --user 1001:1001 -v "$PWD/data/rewayaat:/data" --env-file ./env/rewayaat.env \
    dedicated-rag:0.1.1 python -m drag.cli index --collection-id rewaat \
    >> data/rewayaat/index.log 2>&1 &
  echo "index started, pid $!"
fi
echo "DEPLOY_DONE"
