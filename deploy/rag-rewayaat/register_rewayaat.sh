#!/usr/bin/env bash
# Register the rewayaat provider in rag-registry, allow it for the portal
# consumer, propagate the provider token to registry/gateway env files, and
# recreate registry+gateway so the new env values take effect.
set -euo pipefail
RAG=/home/saber/dedicated-rag
REG=/home/saber/dedicated-rag-registry
GW=/home/saber/dedicated-rag-gateway
BASE=http://127.0.0.1:8200/api/rag-registry
PY=/home/saber/rewayaat/.venv/bin/python3
TOKEN=$(sed -n 's/^DRAG_SERVICE_TOKEN=//p' "$RAG/env/rewayaat.env")
ADMIN=$(sed -n 's/^REGISTRY_ADMIN_TOKEN=//p' "$REG/env/registry.env")
[ -n "$TOKEN" ] || { echo "FATAL: DRAG_SERVICE_TOKEN missing in env/rewayaat.env (run deploy first)"; exit 1; }
[ -n "$ADMIN" ] || { echo "FATAL: REGISTRY_ADMIN_TOKEN missing"; exit 1; }

echo "== [1/5] provider token in registry/gateway envs (idempotent) =="
grep -q '^REWAYAAT_PROVIDER_TOKEN=' "$REG/env/registry.env" || echo "REWAYAAT_PROVIDER_TOKEN=${TOKEN}" >> "$REG/env/registry.env"
grep -q '^REWAYAAT_PROVIDER_TOKEN=' "$GW/env/gateway.env"   || echo "REWAYAAT_PROVIDER_TOKEN=${TOKEN}" >> "$GW/env/gateway.env"

echo "== [2/5] register provider =="
CODE=$(curl -s -o /tmp/rew_rag_get.json -w '%{http_code}' \
  -H "Authorization: Bearer $ADMIN" "$BASE/rewayaat")
if [ "$CODE" = "200" ]; then
  echo "already registered:"; cat /tmp/rew_rag_get.json; echo
else
  cat > /tmp/rew_rag_post.json <<'JSON'
{
 "rag_id": "rewayaat",
 "rag_name": "rewayaat",
 "display_name": "وروایات — کتب حدیث (Al-Kāfī) (mxbai-1024)",
 "provider_type": "dedicated",
 "base_url": "http://127.0.0.1:8107",
 "api_endpoint": "/v1",
 "api_contract_version": "v1",
 "healthcheck_endpoint": "/health",
 "auth_ref": "env:REWAYAAT_PROVIDER_TOKEN",
 "priority": 7,
 "fallback_priority": 100,
 "retrieval_weight": 1.0,
 "retrieval_config": {},
 "embedding_model": "mxbai-embed-large",
 "vector_index": "",
 "top_k": 5,
 "similarity_threshold": 0.05,
 "timeout_ms": 10000,
 "retry_policy": {"backoff_ms": 500, "max_retries": 1},
 "enabled": true,
 "allowed_consumers": ["radio", "portal", "gateway", "pcm23-verify"]
}
JSON
  curl -sS -X POST -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
    --data @/tmp/rew_rag_post.json "$BASE" | tee /tmp/rew_rag_post_out.json
  echo
fi

echo "== [3/5] test-connection =="
curl -sS -X POST -H "Authorization: Bearer $ADMIN" "$BASE/rewayaat/test-connection" || true
echo

echo "== [4/5] allow rewayaat for the portal consumer =="
REG_ADMIN="$ADMIN" "$PY" - <<'PY'
import json, os, urllib.request, urllib.error
admin = os.environ["REG_ADMIN"]
base = "http://127.0.0.1:8200/api/rag-registry"
H = {"Authorization": "Bearer " + admin}

def req(method, path, body=None, extra=None):
    headers = dict(H)
    if body is not None:
        headers["Content-Type"] = "application/json"
    if extra:
        headers.update(extra)
    r = urllib.request.Request(
        base + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        raw = e.read().decode() or "{}"
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"raw": raw}

code, consumers = req("GET", "/consumers")
items = consumers if isinstance(consumers, list) else (
    consumers.get("consumers") or consumers.get("items") or [])
portal = next((c for c in items if c.get("consumer_id") == "portal"), None)
if portal is None:
    print("portal consumer NOT found (GET /consumers ->", code, ")")
    print(str(consumers)[:500])
    raise SystemExit(1)

allowed = sorted(set(portal.get("allowed_rag_ids") or []) | {"rewayaat"})
fields = ("display_name", "description", "default_rag_id", "routing_policy",
          "citation_format", "timeout_ms", "allowed_rag_ids", "stream_allowed",
          "enabled")
body = {k: portal[k] for k in fields if k in portal}
body["allowed_rag_ids"] = allowed
code, out = req("PATCH", "/consumers/portal", body)
if code in (412, 428):  # precondition required -> retry with If-Match
    code, out = req("PATCH", "/consumers/portal", body, extra={"If-Match": "*"})
print("PATCH /consumers/portal ->", code)
print(json.dumps(out, ensure_ascii=False)[:500])
PY

echo "== [5/5] recreate registry + gateway (new env values) =="
cd "$REG" && docker compose -f compose.registry.yml up -d
cd "$GW"  && docker compose -f compose.gateway.yml  up -d
sleep 6
curl -sf "$BASE/health" && echo " <- registry /health" || echo "WARN: registry /health"
echo "REGISTER_DONE"
