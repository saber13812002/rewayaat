# Checkpoint — 2026-11-21 00:16 UTC

## Phase 1: "6 books vs 3326 queries" anomaly — RESOLVED

Reconciliation (live, on s52, `tmp/remote_booktable2.py`):

- Live catalog = **21 books, 50,038 narration docs** (`/v1/browse/books`).
- The snapshot plan is **chapter-granular** for the two giant books (LOOKUP mode
  clamps at 10,000): Al-Kāfi = 2,612 chapter queries, Ghurar al-Ḥikam = 718.
  The "3326 completed" count is completed **chapter/book queries**, not books.
- Raw snapshot so far = **27,869 unique docs (55.7%)**, 29,163 lines, in
  `data/raw/snapshot-20261117-204731-1.jsonl`.
- Per-book status (live / done / fail / raw-uniq / status):

| book | live | done | fail | uniq | status |
|---|---|---|---|---|---|
| Al-Kāfi | 14242 | 2590/2612 | 7 | 13568 | chapter-gap, retrying |
| Ghurar al-Ḥikam | 11012 | 712/718 | 0 | 10935 | in-progress |
| Mīzān al-Ḥikma | 4612 | 1/1 | 0 | 1600+ | in-progress (page 9/24) |
| Man Lā Yaḥḍuruh al-Faqīh | 6379 | 0 | 1 | 0 | failed, retry next tick |
| Mishkāt al-Anwār | 1895 | 1/1 | 1 | 1495 | partial (gap 400, repair pass) |
| Nahj al-Balāgha | 2260 | 0 | 0 | 0 | pending in current pass |
| Al-Amālī / Al-Khiṣāl / Al-Tawḥīd / Kitāb al-Ghayba / Kitāb al-Muʾmin / Kitāb al-Zuhd / Kāmil al-Ziyārāt / Maʿānī al-ʾAkhbār / Muʿjam al-Aḥādīth al-Muʿtabara | — | 0 | 1 | 0 | failed, retry next tick |
| Faḍaʾil al-Shīʿa | 45 | 1/1 | 0 | 45 | COMPLETE |
| Kitāb al-Ḍuʿafāʾ | 226 | 1/1 | 0 | 226 | COMPLETE |
| Risālat al-Ḥuqūq / Thawāb / ʿUyūn / Ṣifāt | — | 0 | 0 | 0 | pending in current pass |

## API degradation (rewayaat.info) — evidence

- Every one of the 18 failed labels failed with
  `HttpError ... failed after 4 attempts (TransportError)`.
- Single-attempt probes (10s connect / 600s read) — `tmp/remote_failprobe2.py`,
  still running in terminal 3: control book `Mīzān al-Ḥikma` (small, already
  complete) → **Read timed out after 601.6s**; `Al-Amālī` → 601.9s;
  `Al-Khiṣāl` → 601.5s. The control proves the degradation is **global**,
  not per-book.
- The live snapshot process is NOT hung: it is progressing at the slowed
  API's pace (~4 min/page observed on Mīzān al-Ḥikma, page 7→9 between
  00:03 and 00:12 UTC); raw mtime advancing, `last_activity` fresh.

## Process / automation ownership (verified)

- The extraction process (PID 777101, `corpus_sync.cli snapshot`, started
  2026-11-17 20:47:31 UTC) **is the MainPID of `sync.service`**
  (`TriggeredBy: sync.timer`, state `activating (start)`).
- `sync.timer` (OnCalendar 00/6:00, Persistent=true) **skips ticks while the
  service is active** (list-timers: NEXT `-`). When this run exits, the timer
  resumes at the next 00:00/06:00/12:00/18:00 boundary.
- `deploy/sync_once.sh` then decides: state has `failed` → `snapshot --resume`
  (retries ONLY the failed labels, idempotent dedupe at finalize); else fresh
  full snapshot.
- `sync-monitor.timer` fires hourly (next 01:00 UTC).
- Local watcher `monitor/extraction_watcher.py` running (terminal 2, 15-min poll).

## Phase 3 LLM infra re-verified (this cycle) — `tmp/remote_llm_check2.py`

Gateway `litellm-personal` :4101, key `GATEWAY_LLM_API_KEY` (= `LITELLM_MASTER_KEY`,
`sk-d58***`, len 67) from the `rag-gateway` container env:

- `/health/liveliness` → 200; `/v1/models` (auth) lists: `gpt-oss-120b`,
  `qwen38-hub`, `q1-embedding-4b`, `q1-embedding-06b`, `rag-mizan`, …
- **chat `gpt-oss-120b` → WORKS** (vLLM 0.28, reasoning model). NOTE: it spends
  part of the completion budget on `reasoning_content` first — with
  `max_tokens=40` the reply came back `finish_reason=length, content=null`;
  with `max_tokens=600` it returned the Persian word `عطوفت`
  (`finish_reason=stop`, 136 completion tokens, 0.7 s). **Pipeline implication:
  request ≥ ~300 completion tokens per call** (or read `reasoning_content`
  correctly) — do not treat null-content as a failure; check `finish_reason`.
- **chat `qwen38-hub` → DOWN** (HTTP 500; upstream 172.20.1.53:8001 refused,
  unchanged since earlier probe). Secondary route only.
- **embeddings → WORK**: `q1-embedding-4b` dims=2560, `q1-embedding-06b`
  dims=1024 (both, 2 inputs → 2 vectors).

## Phase 2 evidence (this cycle)

- Real partial export ran on s52 (commit `fd2f006`): 26,469 docs, 6 books,
  manifest + self-hash verified, raw.jsonl byte-identical.
- **ES smoke test PASS on pct-ES 8.13.4** (`tmp/es_smoke_test.py`,
  `ES_SMOKE_PASS`): throwaway index created from shipped
  `exporter._es_mapping()` (ICU `arabic_normalization`, `synonym_graph`,
  kstem all accepted); doc `Al-Kafi-Volume-2-Kulayni:955` indexed twice →
  count stays 1 (idempotent by `_id=source_id`); `match` on `arabic`
  (vocalized `عِدَّةٌ` → hit, diacritics folded by analyzer); `match` on
  `english` → hit; `book`+`hadith_number` keyword filter → hit.
  (Note: `term` on text fields does NOT work on vocalized input — use `match`.)

## Next steps

1. Watcher keeps polling; when `sync.service` exits (this pass + repair pass
   + finalize), the next timer tick resumes the failed labels.
2. When a snapshot finalizes with `state["failed"] == []` and
   `incomplete == []`: re-run `export` (status=complete), validate counts,
   build final SQLite baseline (Phase 1 done).
3. If rewayaat.info stays degraded past several ticks, escalate: report as
   upstream blocker with the control-timeout evidence above.
