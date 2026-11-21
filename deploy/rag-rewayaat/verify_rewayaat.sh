#!/usr/bin/env bash
# End-to-end verification of the rewayaat RAG on s52.
set -uo pipefail
RAG=/home/saber/dedicated-rag
TOKEN=$(sed -n 's/^DRAG_SERVICE_TOKEN=//p' "$RAG/env/rewayaat.env")
ADMIN=$(sed -n 's/^REGISTRY_ADMIN_TOKEN=//p' /home/saber/dedicated-rag-registry/env/registry.env)
BASE=http://127.0.0.1:8200/api/rag-registry

echo "== containers =="
docker ps -a --format '{{.Names}}\t{{.Status}}' | grep -E 'rewayaat' || echo "(no rewayaat containers)"

echo "== provider /health =="
curl -sf http://127.0.0.1:8107/health; echo

echo "== provider /v1/stats (auth) =="
curl -sf -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8107/v1/stats; echo

echo "== provider /v1/search (auth) =="
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"query":"دعا برای سلامتی","top_k":3}' http://127.0.0.1:8107/v1/search | head -c 700; echo

echo "== ingest/index progress =="
echo "--- ingest.log (tail) ---"; tail -2 "$RAG/data/rewayaat/ingest.log" 2>/dev/null || echo "(none)"
echo "--- index.log (tail) ---"; tail -3 "$RAG/data/rewayaat/index.log" 2>/dev/null || echo "(none)"

echo "== shim /health =="
curl -sf http://127.0.0.1:8130/health; echo

echo "== registry row =="
curl -s -H "Authorization: Bearer $ADMIN" "$BASE/rewayaat" | head -c 900; echo

echo "== public portal /api/rag/sources =="
curl -s -o /tmp/rew_sources.json -w 'http_code=%{http_code}\n' \
  http://rag.ai.ismc.ir/api/rag/sources
head -c 1200 /tmp/rew_sources.json; echo
grep -c rewayaat /tmp/rew_sources.json 2>/dev/null && echo "rewayaat present in public sources" || echo "rewayaat NOT in public sources yet"
