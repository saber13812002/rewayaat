# Phase 3 Checkpoint — LLM Enrichment: live-verified, full run launched (2026-11-21)

## Status: RUNNING (full background enrichment on s52)

## What is running

- **Process**: PID `1612434` on s52 (server 133, 172.20.1.52, ~/rewayaat), `.venv/bin/python -m corpus_sync.cli enrich`
- **Snapshot**: `data/raw/snapshot-20261117-204731-1.jsonl` (still growing — extraction PID 777101 under sync.service; 30,866 lines at 01:28 UTC, ~61.7% of live 50,038)
- **Output dir**: `data/out/enrich/snapshot-20261117-204731-1/`
  - `enrichment.jsonl` (append-only, one record per source_id, fsync per record)
  - `enrich_progress.log` (machine-parseable `[ts] LEVEL [n/total] sid status/error`)
  - `enrich_state.json` (per-sid checkpoint; flushed every 25 docs + at exit)
  - `summary.json` (written at end: status, totals, failed_ids)
- **Background stdout**: `data/enrich_bg.log`

## Launch command (exact)

```
ssh s52 "cd ~/rewayaat && LLM_API_KEY=sk-d58f7724e2e3167efde8b6c5e09a7a9b1c38b98938aeb2b68781d7e643e9ef06 PYTHONIOENCODING=utf-8 nohup .venv/bin/python -m corpus_sync.cli enrich --raw data/raw/snapshot-20261117-204731-1.jsonl --snap snapshot-20261117-204731-1 >> data/enrich_bg.log 2>&1 & echo RESTARTED"
```

Key is also persisted at `/tmp/enrich_smoke_key` on s52.

## Resume semantics

`resume=True` default: sids present in `enrich_state.json` (status ok) are skipped;
failed sids are retried; `enrichment.jsonl` is append-only and `load_enrichment`
gives later lines per source_id the win, so duplicate reprocessing is harmless.
Killing the process loses at most the last ≤24 state flushes — those sids are
re-processed, not corrupted.

## 5-doc live smoke (PASS, 2026-11-20 ~23:50 UTC)

- 5/5 pass, 55.1s total, model `gpt-oss-120b`, prompt_version 1.0, prompt_hash `258a3166bb22`
- tokens 1323–1616 per doc, finish_reason=stop, validation status=pass on all 5
- export merge verified: `enrichment.persian == top-level persian`; manifest
  `enrichment: {"source": ..., "enriched_docs": 5}`
- artifacts: `/tmp/enrich_smoke_out/`, `/tmp/enrich_smoke_export/`, `/tmp/enrich_smoke_raw.jsonl`

## Truncation bug — root cause + fix (commit 7ea9ada)

Full run at base budget 2048 showed ~42% `ValueError: unbalanced JSON object in
LLM output`. Probe (`tmp/remote_parse_fail_probe.py`, 2026-11-21 ~01:15 UTC):

```
Al-Kafi-Volume-3-Kulayni:1 | budget=2048 | finish=stop | tokens=1770 | latency=10149ms | PARSE_OK
Al-Kafi-Volume-3-Kulayni:1 | budget=8192 | finish=stop | tokens=1769 | latency=10168ms | PARSE_OK
Al-Kafi-Volume-1-Kulayni:1 | budget=2048 | finish=length | tokens=2048 | latency=12102ms | PARSE_FAIL | tail='...تُوَحِّدَ بِالْجَبْرُوتِ، وَبِحِكْمَتِهِ أَظ'
Al-Kafi-Volume-1-Kulayni:1 | budget=8192 | finish=length | tokens=8192 | latency=48986ms | PARSE_FAIL | tail='...این نیز به ارادهٔ اوست؛ پس' (mid-Persian)
Al-Kafi-Volume-8-Kulayni:1 | budget=2048 | finish=length | tokens=2048 | latency=11113ms | PARSE_FAIL | tail='ran_references": [\n    {\n      "surah": 7,\n      "ayah": 165'
Al-Kafi-Volume-8-Kulayni:1 | budget=8192 | finish=stop | tokens=2040 | latency=11853ms | PARSE_OK
```

`gpt-oss-120b` is a vLLM reasoning model: completion budget is shared between
reasoning and output tokens, so long docs exhaust it with
`finish_reason=length` while content is non-null (truncated mid-JSON). Old
`chat()` only escalated budget on **null** content; truncated content was
returned and failed JSON parsing downstream.

Fix (7ea9ada, deployed to s52, full suite 132/132):
- `enrich.py`: `chat()` now raises `_BudgetExhausted` on ANY `finish_reason=length`
  (null or truncated content) → doubled budget; `MAX_BUDGET` 8192 → 16384
- `config.py`: `LLM_MAX_TOKENS` 2048 → 4096 (base); `LLM_REQUEST_TIMEOUT_S` 120 → 300
- new tests: `test_chat_truncated_content_budget_escalation`,
  `test_chat_truncated_exhausts_retries`

Post-fix live verification (first docs of relaunched run, 01:28–01:29 UTC):

```
[2026-11-21T01:28:29Z] INFO [1/30866] Al-Kafi-Volume-3-Kulayni:1 pass
[2026-11-21T01:29:25Z] INFO [2/30866] Al-Kafi-Volume-1-Kulayni:1 pass   <- needed 16384 (56s)
[2026-11-21T01:29:35Z] INFO [3/30866] Al-Kafi-Volume-8-Kulayni:1 pass   <- needed 8192
[2026-11-21T01:29:46Z] INFO [4/30866] Al-Kafi-Volume-2-Kulayni:1 pass
```

ERROR rate in new run: 0/4 (was ~42% pre-fix). Remaining ~16384-exhaustion
failures will be retried on resume passes.

## Throughput estimate

~11 s/doc sequential at 4096 base (latency scales with generated tokens, not
the cap; escalated docs cost extra):

- 30.5k current docs ≈ **94 h** (~3.9 days)
- final 50,038 docs ≈ **154 h** (~6.4 days)

Sequential by design (single GPU on litellm-personal; concurrency would risk
429s and ordering guarantees are unnecessary since state is per-sid).

## Remaining Phase 3 steps

1. Track to completion: watch `enrich_progress.log`; hourly report cadence.
   Health check: `ssh s52 "tail -3 ~/rewayaat/data/out/enrich/snapshot-20261117-204731-1/enrich_progress.log"`
2. When run finishes with `status: partial` (it will — the snapshot was still
   growing at start), re-run the SAME launch command after extraction
   finalizes; resume covers only the newly-appended docs.
3. After final `status: complete`, re-run Phase 2 export
   (`corpus_sync.cli export`) so the final JSONL/Parquet/ES bundle carries
   the `enrichment` field for all docs.
4. Optional: `--embed` pass (`run_embeddings`, q1-embedding-06b, dim 1024,
   batch 16) once enrichment is complete — needed for Phase 4 vector search.
