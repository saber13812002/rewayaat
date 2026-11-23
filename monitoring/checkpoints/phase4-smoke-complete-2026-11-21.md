# Phase 4 live smoke — COMPLETE (2026-11-21, 08:30 UTC)

Server 133 (`ssh s52`), 50-doc slice `phase4-smoke-50` (first 50 enriched
sids of snapshot-20261117-204731-1). Script:
`~/rewayaat/data/tmp/phase4-smoke/phase4_smoke.sh` (10 steps).
Commit under test: `fb9c935` (branch `corpus-sync-pipeline`).

## Result: PHASE4_SMOKE_COMPLETE — all 10 steps green

| Step | Check | Evidence |
|---|---|---|
| 1 | prep smoke slice | 50 raw lines, 50 enrichment records |
| 2 | embed (one-text-per-request, WORKERS=1) | 50/50 vectors in `data/out/enrich/phase4-smoke-50/embeddings.jsonl`; 0 failures; resumable (run #7: 0 to do) |
| 3 | load-es | index `rewayaat_corpus_v1` created, 50 docs + 50 vectors, 0 errors |
| 4 | load-es re-run (idempotency) | same count, 0 errors, index PUT exactly once |
| 5 | load-graph + deterministic related pass | 50/50 loaded, `RELATED_TO: 81` created |
| 6 | ES + graph stats | ES count=50; graph labels: Narration 50, Book 8, Chapter 16, Narrator 98, QuranVerse 46, Topic 351, Entity 461; rels: IN_BOOK 50, IN_CHAPTER 50, HAS_CHAPTER 16, NARRATED_BY 219, MENTIONS_ENTITY 598, HAS_TOPIC 308, CANDIDATE_TOPIC 150, REFERENCES 51, RELATED_TO 81 |
| 7 | ES direct multi_match "justice" + kNN | 3 hits (top: Al-Kafi-Volume-5-Kulayni:6, 2.64); `KNN_PASS` — 5 hits, cosine 0.70/0.69/0.69/0.69/0.69, all `has_vector: true` |
| 8 | RAG trilingual EN/AR/FA | 3 answers produced (EN/AR/FA), each with a provenance log under `data/out/rag/`; answers grounded in the 50-doc context (e.g. "two just (ذوا عدل) … are the Muslims" from Al-Kāfi vol 1/7); citations verified where emitted |
| 9 | Neo4j traversal | Book→chapter→narration counts (7/7/6/6/6), top narrators (أَبِي عَبْدِ اللَّهِ 23, عَلِيُّ بْنُ إِبْرَاهِيمَ 13, …), IN_CHAPTER source addresses incl. part/chapter/hadith_number, top topics; `TRAVERSAL_PASS` |
| 10 | done | `PHASE4_SMOKE_COMPLETE` |

## Bugs found and fixed live (evidence over code)

1. **Embed gateway concurrency limit (Phase 3 carry-over, fully characterized)**
   - multi-text `POST /v1/embeddings` (n≥2) → HTTP 408 at ~46.3s (backend timeout ~45s)
   - 2 concurrent single-text requests → 22/50 failures (408 + 500)
   - **strictly sequential single-text requests → 22/22 in 95s, 0 failures** (≈4.3s each under load)
   - `embed_single.py` pinned to `WORKERS = 1`; full-corpus 32k-doc embed pass must use the same path (≈13s/doc single probe → plan the long run accordingly).
2. **Neo4j MERGE-in-UNWIND shared-node duplicate creation** (commit `5fed205`)
   - `ConstraintValidationFailed: Node(50) already exists with label Book and property book_key = 'Al-Kāfi|6'` — all 50 rows of a batch MERGE the same Book; Cypher evaluates every find phase before writes, so on a fresh graph all N rows CREATE the node.
   - Fix: split each combined statement into a node-creation statement (MERGE by unique key, rows **deduped** per batch) and a relationship statement (MATCH both endpoints + MERGE rel — idempotent). `_exec_batch` runs node phase → rel phase.
3. **ST_REL_IN dropped every row (IN_BOOK/IN_CHAPTER = 0 in run #6)** (commit `fb9c935`)
   - `ST_REL_IN` MATCHes `Chapter {chapter_key: r.chapter_key}` but `in_book` rows only carried `(sid, book_key)` → matched on `chapter_key: null` → all 50 rows silently dropped.
   - Fix: `chapter_key` added to `in_book` rows + its dedup key; regression test `test_statements_reference_only_emitted_row_fields` audits that every `r.<f>` read by any statement is present on every emitted row of that kind (would have caught this before deploy).

## Infra findings

- **Neo4j auth**: container `neo4j_db` env is `NEO4J_AUTH=neo4j/your_neo4j_password_here` — the `.env` template placeholder **is** the literal password. `~/saberprojects/graph-rag/.env` sets compose-internal `NEO4J_URI=bolt://neo4j:7687` (unresolvable from host) and `NEO4J_USERNAME` (config reads `NEO4J_USER`) → smoke script exports `NEO4J_URI=bolt://127.0.0.1:7687`, `NEO4J_USER=neo4j` after sourcing the .env.
- **LLM gateway** (litellm-personal @ 127.0.0.1:4101): chat `gpt-oss-120b`, embed `q1-embedding-06b` (dim 1024).
- **ES 8.13.4** @ 127.0.0.1:9200 (no auth); index `rewayaat_corpus_v1` holds the 50 smoke docs + vectors.

## Notes / remaining

- Smoke graph is on a partially-populated-then-merged graph; MERGE idempotency was exercised for real (re-run of load-graph after clearing `graph_state.json` succeeded against existing nodes; load-es re-run idempotent).
- RAG answers on this 50-doc slice are context-limited (the slice has few "justice" narrations) — retrieval quality to be judged on the full corpus in Phase 5.
- One citation check reported `partial_citation` (LLM paraphrased the citation token); verifier correctly flags it and the answer's provenance log records the drop. Acceptable behavior; tracked for Phase 5 citation-correctness testing.
