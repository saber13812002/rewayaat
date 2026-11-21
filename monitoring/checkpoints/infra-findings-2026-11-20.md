# Infrastructure Findings — s52 (server 133, git.ai.ismc.ir 172.20.1.52)
Checkpoint: 2026-11-20 23:07 UTC. Source: live probes + container env inspection.

## Extraction (Phase 1)
- In-progress snapshot: `snapshot-20261117-204731-1` (PID 777101, `.venv/bin/python -m corpus_sync.cli snapshot`).
- State keys: `snapshot`, `completed` (dict label→pages), `totals`, `repair_tried`, `sha256`, `failed` (list), `last_activity`.
- Progress at 23:09Z: completed=3305 / totals=3326; 17 failed labels (transient TransportError) — all small books + a few Al-Kāfi chapters. Will retry on next `--resume`.
- Raw file: `data/raw/snapshot-20261117-204731-1.jsonl` = 27,563 lines / 150.8 MB (growing).
- SQLite `data/db/corpus_sync.db` is EMPTY until finalize (expected — records stored only after `run_snapshot` returns).
- `data/out/` has a stale partial summary `snapshot-20261117-192628-1.summary.json` (docs=45, prior scoped run) — NOT the current snapshot; ignore until current finalize.
- Automation: `sync.timer` (00/06/12/18 UTC) → `sync.service` → `deploy/sync_once.sh` (resume-aware). If current run finalizes with failed queries, next tick auto-`--resume`s only failed labels.
- Watcher: `monitor/extraction_watcher.py` (local, 15-min poll via ssh, hard cap 96h) writes `monitoring/status.json`, `monitoring/history.jsonl`, hourly `monitoring/hourly/<ts>.{txt,json}`, stops on DB `status=complete|partial`.

## LLM (litellm-personal :4101)
- Endpoint: `http://127.0.0.1:4101/v1/...` (OpenAI-compatible).
- **Working key** = `LITELLM_MASTER_KEY` from `litellm-personal` container env (== `GATEWAY_LLM_API_KEY` on rag-gateway), 67 chars, `sk-` prefixed. NOT the `litellm_config.yaml` "master_key" (that grep returned empty; the yaml uses per-model `api_key: os.environ/...`).
- Model list (this key): `qwen38-hub`, `gpt-oss-120b`, `q1-embedding-4b` (dim 2560), `q1-embedding-06b` (dim 1024), `rag-mizan`, `rag-general`, `rag-fiqh`, `portal-rag-*`, `bootstrap-do-not-use`.
- **Functional results:**
  - `gpt-oss-120b` chat: **WORKS** — returns clean text, good Persian (tested: "هیچ معبودی جز خدا نیست؛ محمد، پیامبر خداست.").
  - `qwen38-hub` chat: **DOWN** — upstream `172.20.1.53:8001` ConnectionRefused (litellm log: Connect call failed). 500 on every call; no fallback. This is the model rag-gateway currently uses → rag-gateway is likely also degraded right now.
  - `qwen38` / `qwen3.8-27b-uncensored-h100` / `bge-m3`: **invalid model names** for this gateway (400) — those are from the yaml, not the active gateway routing.
  - `q1-embedding-4b`: **WORKS** dim=2560. `q1-embedding-06b`: **WORKS** dim=1024.
- **Decision for Phase 3:** primary chat model = `gpt-oss-120b` (verified). `qwen38-hub` is a fallback *if* 172.20.1.53:8001 recovers (re-probe hourly via watcher). Embeddings: `q1-embedding-4b` (2560-d, multilingual) for the RAG vector index; note prod ES uses 1024-d `semantic_vector` — for a NEW index on s52 we can pick 2560 freely.

## Neo4j (Phase 4)
- **No running Neo4j container**, but a full project exists: `/home/saber/saberprojects/graph-rag/`.
  - `docker-compose.yml`: `neo4j:2025.10.1-enterprise`, container `neo4j_db`, ports 7474/7687, APOC enabled + export/import file enabled, volume `./neo4j_data:/data`, `./neo4j_dump:/import`.
  - Existing data: `neo4j_data/` (neo4j + system DBs), backup tar `neo4j_data-backup-20261002-0225.tar.gz`.
  - `NEO4J_AUTH=neo4j/${NEO4J_PASSWORD}` (password in `.env`), `NEO4J_DATABASE=neo4j`.
  - App: `mrbaqeri/qa_lang_neo4j_mesbah:1.0.0` (langgraph + neo4j 5.28.2 + langchain) — a separate QA/graph app, not the rewayaat corpus loader.
- **Decision:** bring up `neo4j_db` (existing container, additive) for the KG; do NOT rebuild. Provision as a new service under the rewayaat pipeline, load a NEW graph (book/chapter/hadith/narrator/verse nodes), keep the existing `neo4j` DB data intact (use a separate database name if isolation is required).

## Elasticsearch (Phase 4)
- **pct-elasticsearch 8.13.4** at 127.0.0.1:9200, no auth, 2GB heap. Data bind-mount `/home/saber/saberprojects/persian-corpus-toolkit/data/es`. Current index `knowledge_alghadir-allame-amini_v0001` (92,417 docs) — unrelated to rewayaat.
- **Decision:** create a NEW index `rewayaat_multilingual` (v0001) on this ES for the trilingual RAG docs; leave existing indices untouched.
- Production rewayaat ES (k8s `elasticsearch-v2.elastic-v2.svc.cluster.local`, alias `rewayaat_hadith`) is NOT reachable from s52 (no kubectl). Out of scope for the s52 RAG build; noted in report.

## Disk / RAM
- `/` = 686G total, 279G used, **380G free**. RAM: ~49Gi free. Plenty for ES + Neo4j + embeddings.
