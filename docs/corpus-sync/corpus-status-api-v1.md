# Corpus Status API — Contract v1.0

Status: **active**. Versioned contract shared by both coordination projects:

| Project | Role | Location |
|---|---|---|
| `rewayaat` (agent 1) | **provider** — implements & serves this API | [`corpus_sync/statusapi.py`](../../corpus_sync/statusapi.py) |
| `persian-corpus-toolkit` (agent 2) | **consumer** — dashboard at `corpus.ai.ismc.ir` reads it | (mirror of this doc in the toolkit repo) |

**Change rule:** any breaking change bumps `api_version` (semver). Additive
fields are allowed in PATCH releases. Both repos must carry a copy of this
document at the matching version; the provider's `GET /` advertises the
version it serves. Consumers must tolerate missing/additional fields
(unknown `status` strings ⇒ render as "unknown", never as "ok").

---

## 1. Transport

- Protocol: HTTP, JSON (`application/json; charset=utf-8`), GET only.
- Base URL (production, s52): `http://172.20.1.52:8190` (service
  `statusapi.service`, user systemd, port **8190**).
- No authentication (read-only, internal network). Every response is
  computed from live sources at request time — **no cached/fabricated
  numbers**. `Cache-Control: no-store` is set on all responses.
- Errors: `404 {"error":"not_found"}` for unknown paths, `500` with an
  `error` string on internal failure. A *source* being down is **not** a
  500 — it is reported inside the stage object as `status: "down"`/
  `"unknown"` with an `error` field (the contract: the API reports the
  state of the world, including its inability to see it).

## 2. Endpoints

| Path | Purpose |
|---|---|
| `GET /` | service name, `api_version`, `corpus_id`, endpoint list |
| `GET /health` | liveness probe: `{status, api_version, corpus_id, uptime_s}` |
| `GET /v1/status` | full status document (schema below) |
| `GET /v1/summary` | compact card for dashboard tiles (counts, progress %, per-stage status, overall health, rates) |
| `GET /v1/history?limit=N` | hourly persisted counters (`data/status/history.jsonl`), newest last |
| `GET /v1/errors` | `enrichment_failed` count + recent ERROR lines from checkpoints/logs |

## 3. `GET /v1/status` schema

All `status` values are drawn from this controlled vocabulary:
`ok | running | complete | partial | paused | waiting | idle | degraded |
unknown | not_staged | not_published | auth | down | failed`.
`unknown` = source not reachable/inspectable (≠ `down` = reachable but
responding as unhealthy/absent).

```jsonc
{
  "api_version": "1.0",
  "corpus_id": "rewaat",                 // stable corpus identifier
  "generated_at": "2026-11-23T16:00:00Z",
  "pipeline": {
    "extraction": {                       // snapshot from live source API
      "status": "complete",
      "snap_id": "snapshot-20261117-204731-1",
      "created_at": "…", "total_docs": 31531,
      "book_count": 9,
      "books": {"Al-Kafi-Volume-1-Kulayni": 1234, "…": 0},  // per-book counts
      "last_activity": "…",                // from snapshot state file
      "error": null                        // only when source unreadable
    },
    "enrichment": {
      "status": "running",                 // running|complete|partial|paused|idle|unknown
      "snap_id": "…", "total_docs": 31531,
      "enriched": 31488, "failed": 46,
      "progress_pct": 99.86,
      "last_activity": "…",                // checkpoint timestamp
      "workers": 3,                        // live enrich processes (pgrep)
      "state_file": "data/out/enrich/<snap>/enrich_state.json",
      "recent_errors": ["[ts] ERROR [i/n] <source_id> <Exc>: …", "…"]
    },
    "embedding": {
      "status": "running",                 // running|complete|waiting|paused|unknown
      "chunks_total": 34369, "chunks_embedded": 12345,
      "progress_pct": 35.9,
      "version": "rewayaat_v1", "index_status": "building",
      "embed_model": "Qwen3-Embedding-0.6B", "embed_dim": 1024,
      "embed_version": "qwen3-embed-06b-v1",
      "supervisor_running": true, "index_workers": 1,
      "recent_log": ["…last non-empty lines of the index log…"]
    }
  },
  "stores": {
    "elasticsearch": {
      "status": "ok", "index": "rewayaat_corpus_v1",
      "docs": 31188, "latency_ms": 12.3
    },
    "neo4j": {
      "status": "ok", "nodes": 1030, "relationships": 4210
      // or: {"status":"down","error":"…"} / {"status":"unknown","error":"…"}
    },
    "huggingface": {
      "repo": "saber13812002/rewayaat-corpus", "is_private": true,
      "status": "staged | not_staged | repo_exists_remote | not_published | auth",
      "manifest": "…relative path…", "staged_at": "…", "docs_total": 31531
    }
  },
  "services": {
    "rag_provider": {"status": "ok", "url": "http://127.0.0.1:8107",
                     "latency_ms": 8.1},
    "embedder": {"status": "ok", "url": "http://172.20.1.53:4001/v1/models",
                 "latency_ms": 40.0, "models": ["Qwen3-Embedding-0.6B"]},
    "git": {"status": "ok", "head": "4b0ecd9", "branch": "corpus-sync-pipeline",
            "dirty_files": 0}
  },
  "rates": {
    "enrichment_per_hour": 888.0,          // or null when < 5 min of samples
    "embedding_per_hour": 1920.0,
    "window_note": "computed from 60s in-memory poll window since process start; null if < 5 min of samples"
  },
  "errors": {
    "enrichment_failed": 46,
    "recent": {"enrichment": ["…"], "embedding_log": ["…"]}
  },
  "health": {
    "overall": "ok | degraded | down",
    "checks": {"extraction": "complete", "enrichment": "running",
               "embedding": "running", "elasticsearch": "ok",
               "neo4j": "ok", "rag_provider": "ok", "embedder": "ok"}
  }
}
```

`health.overall` mapping (worst check wins): severity 0 = ok/complete/
running/staged/repo_exists_remote; 1 = partial/auth/paused/waiting/degraded;
2 = unknown/idle/not_staged/not_published; 3 = down/failed.

## 4. History & reports (dashboard items 8/9)

- The poller samples (enriched, embedded, ES docs) every 60 s into an
  in-memory window (≤ 24 h) and appends one record per UTC hour to
  `<data_dir>/status/history.jsonl` (this file is committed/backup-able):
  `{ts, window_minutes, counts{enriched,embedded,es_docs}, rates_per_hour{…}}`.
- `GET /v1/history?limit=96` returns the last `limit` records (newest
  last). `GET /v1/errors` feeds the error-viewing panel. Both are
  downloadable as-is (the dashboard may render a download link).
- `rates.*` are derived from the in-memory window: null until the process
  has ≥ 5 minutes of samples (restart ⇒ rates resume, history file persists).

## 5. Multi-corpus extension (item 10: new corpus without code changes)

One process instance = one corpus, selected entirely by environment:

| Env var | Meaning |
|---|---|
| `STATUS_CORPUS_ID` | `corpus_id` in responses (default `rewaat`) |
| `STATUS_DATA_DIR` | corpus pipeline `data/` dir (raw, db, out, state) |
| `STATUS_REPO_ROOT` | deployment clone root (for `git` health) |
| `STATUS_PORT` / `STATUS_HOST` | bind (default `0.0.0.0:8190`) |
| `STATUS_DRAG_DB` / `STATUS_DRAG_LOG` | drag RAG sqlite + index log |
| `ES_URL` / `ES_INDEX` | Elasticsearch count source |
| `NEO4J_CONTAINER` / `NEO4J_USER` / `NEO4J_PASSWORD` (or `NEO4J_ENV_FILE`) | graph counts |
| `RAG_PROVIDER_URL` / `RAG_PROVIDER_TOKEN` | RAG health probe |
| `STATUS_EMBEDDER_URL` | embedding service `/v1/models` probe |
| `HF_REPO_ID` / `HF_IS_PRIVATE` / `HF_TOKEN` | HF dataset status |
| `STATUS_POLL_S` | poll interval (default 60) |

A new corpus therefore needs only a new env file + service unit pointing at
its `data/` dir and backends — no code edits. The central dashboard
registers corpus base URLs in its own config and renders the standard
per-corpus pages from this same schema.

## 6. Provider deployment (s52, rewayaat corpus)

- Service: user systemd unit `statusapi.service` (template in
  [`deploy/statusapi.service`](../../deploy/statusapi.service)), running
  `~/rewayaat/.venv/bin/python -m corpus_sync.statusapi` with
  [`deploy/statusapi.env`](../../deploy/statusapi.env).
- Backups: `deploy/backup_status.sh` (hourly systemd timer) snapshots the
  critical state files (see that script's header) — the status API itself
  is stateless beyond `data/status/history.jsonl`.

## 7. Changelog

- **v1.0** (2026-11-23): initial contract — 6 endpoints, stage schema,
  controlled status vocabulary, history/errors endpoints, multi-corpus env
  model. Provider: `corpus_sync/statusapi.py`.
