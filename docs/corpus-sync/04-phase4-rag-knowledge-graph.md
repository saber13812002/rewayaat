# Phase 4 — RAG + Knowledge Graph (design)

Date: 2026-11-21 · Owner: corpus_sync pipeline · Status: design locked, implementing

## Mission requirements (recap)

- Use the EXISTING Elasticsearch (8.13.4, `pct-elasticsearch`, 127.0.0.1:9200) and
  Neo4j (2025.10.1-enterprise, container `neo4j_db`, 127.0.0.1:7687/7474).
- Schema/mappings + multilingual docs + embeddings.
- Graph links across narrations / Quran verses / books / narrators.
- Preserve hadith numbers + exact source addresses everywhere.
- Trilingual retrieval (FA/AR/EN) and precise citations for LLM answers.
- LLM output never authoritative; everything reviewable/traceable.

## Infrastructure facts (verified 2026-11-21 01:30–01:40 UTC on s52)

| Component | State |
|---|---|
| ES 8.13.4 | running, no auth; only unrelated index `knowledge_alghadir-allame-amini_v0001` (92,417 docs) — untouched |
| Neo4j 2025.10.1-enterprise | was stopped; started via `~/saberprojects/graph-rag` compose (neo4j service only). Data store is EMPTY (1.1M, fresh from 2026-09-29). APOC available. Auth `neo4j` / `<NEO4J_PASSWORD from graph-rag/.env>` |
| Embeddings | `q1-embedding-06b`, dim 1024, via litellm-personal 127.0.0.1:4101 (key in `/tmp/enrich_smoke_key`) |
| venv deps | added `neo4j==5.28.8`, `elasticsearch==8.19.3` to `~/rewayaat/.venv` (enrich run unaffected — it only uses `requests`) |

## Data sources (shapes verified)

Raw snapshot line (`data/raw/snapshot-*.jsonl`):

```
{book, fetched_at, label, page, source_id,
 raw: {_id, book, number, part, part_ar, chapter, section, volume, tags,
       topic_tags, arabic, english, source, gradings, related, history,
       englishContent,
       chain_links:    [{p, s, t, e, g}],   # p=narrator id (n000966), g=slug
       chain_links_en: [{p, s, t, e, g}],
       arabicChain}}
```

Enrichment record (`data/out/enrich/<snap>/enrichment.jsonl`, keyed by `source_id`):

```
{source_id, persian,
 entities: [{type, value, note}], topics: [str],
 quran_references: [{surah, ayah, ref_type, span?}],
 hadith_candidates: [{topic, reason}],
 provenance: {model, prompt_version, prompt_hash, latency_ms,
              completion_tokens, finish_reason, validation{status,issues},
              generated_at}}
```

Note: `hadith_candidates` are **topic-based suggestions, not hadith ids** —
they become graph edges via a deterministic second pass, never LLM-trusted
links to specific narrations.

## ES index: `rewayaat_corpus_v1`

- Mapping = `exporter._es_mapping()` (arabic_norm / english_fold /
  english_fold_search analyzers + translit_names synonyms + strip_hamza,
  nested `chain_links`, disabled-object raw/provenance) **plus**:
  - `semantic_vector`: `dense_vector`, dims 1024, int8_hnsw (m=16,
    ef_construction=100), cosine — copied from `scripts/search/v2_mapping.json`
  - `persian` stays `{"type": "text"}` (standard analyzer; ES 8.13 has no
    stable Persian analyzer — documented tradeoff, adequate for match queries)
- `doc id = source_id` (e.g. `Al-Kafi-Volume-3-Kulayni:2`) → exact source
  address is the identity; hadith number preserved in `hadith_number`
  (raw `number`) + full `book/volume/part/chapter/section` fields.
- Idempotency: `PUT /_doc/{source_id}` per doc (bulk with op_type=index) —
  re-running the loader is a no-op except for changed fields.
- Build input: `exporter.build_export_doc(line, snap_id)` +
  `enrich.apply_enrichment` (persian/topics/quran_references/entities/
  hadith_candidates/provenance) + `semantic_vector` from
  `data/out/enrich/<snap>/embeddings.jsonl` (Phase 3 `--embed` output,
  `build_embed_text = ar[:3000] || en[:300] || fa[:500]`).

## Neo4j schema

Constraints (unique):

```
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Narration) REQUIRE n.source_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Narrator) REQUIRE n.narrator_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (b:Book)     REQUIRE b.book_key IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter)  REQUIRE c.chapter_key IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (v:QuranVerse) REQUIRE v.verse_key IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (t:Topic)    REQUIRE t.value IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity)   REQUIRE e.entity_key IS UNIQUE;
```

Labels + properties:

| Label | Key | Properties |
|---|---|---|
| `Narration` | `source_id` | hadith_number, book, volume, part, chapter, section, arabic, english, persian, arabic_hash, record_hash, snapshot_id, enriched(bool), validation_status |
| `Book` | `book_key` = book+volume | book, volume, source |
| `Chapter` | `chapter_key` = book+volume+part+chapter+section | part, part_ar, chapter, section |
| `Narrator` | `narrator_id` (= chain `p`, e.g. `n000966`) | name_ar (t), name_en (t from chain_links_en), slug (g) |
| `QuranVerse` | `verse_key` = `{surah}:{ayah}` | surah, ayah |
| `Topic` | `value` | value (normalized lowercase-ascii of the English topic) |
| `Entity` | `entity_key` = type+":"+normalized value | type, value, note |

Relationships (all MERGE → idempotent):

```
(:Narration)-[:IN_BOOK]->(:Book)
(:Narration)-[:IN_CHAPTER]->(:Chapter)
(:Book)-[:HAS_CHAPTER]->(:Chapter)
(:Narration)-[:NARRATED_BY {position}]->(:Narrator)     # chain order by s-offset
(:Narration)-[:MENTIONS_ENTITY]->(:Entity)
(:Narration)-[:HAS_TOPIC]->(:Topic)
(:Narration)-[:REFERENCES {ref_type, span}]->(:QuranVerse)
(:Narration)-[:CANDIDATE_TOPIC]->(:Topic)                # from hadith_candidates
(:Narration)-[:RELATED_TO {score, via}]->(:Narration)    # deterministic 2nd pass
```

`RELATED_TO` pass (no LLM): for each enriched narration, find top-5 other
narrations sharing ≥2 topics (exact topic match) — symmetric, capped, with
`via: "topics"` and the shared-topic list as `score` payload. This realizes
"candidate links to other hadith" from Phase 3 without letting LLM guesses
become asserted facts.

Load strategy: Python `neo4j` driver, `UNWIND $rows AS r MERGE ...` batches
(1000 rows/batch), single session, per-record checkpoint file
`data/out/graph/<snap>/graph_state.json` (last source_id processed). Raw
snapshot is read-only input; graph is fully rebuildable
(`MATCH (n) DETACH DELETE n` + reload = idempotent re-run).

## RAG query path (CLI: `corpus_sync.cli rag "query" --lang auto`)

1. **Retrieve (ES)**: hybrid per query language + cross-language:
   - `multi_match` over `arabic`/`english`/`persian` (+ `chapter`, `book`)
   - `kNN` on `semantic_vector` (query text embedded with the same model)
   - fusion: RRF (reciprocal rank, k=60) over the two result lists, top-20
   - optional structured filters: `--book`, `--hadith`, `--volume`
2. **Expand (Neo4j)**: for the top hits, 1-hop context pull:
   narrators, topics, Quran verses, RELATED_TO neighbors — merged into the
   candidate set (capped), each candidate carrying its full citation.
3. **Answer (LLM via litellm gateway)**: strict system prompt — answer in the
   query language, cite only from the provided context, citation format:
   `⟦book · vol. V · part · chapter · hadith N⟧` + `source_id`.
4. **Citation check**: every `⟦…⟧` citation is verified against the context
   set (book/volume/hadith match) → `citations_verified: n/m`; unverified
   citations are dropped and the answer marked `partial_citation`.
   Provenance recorded: model, prompt_version, prompt_hash, latency,
   context list with scores — appended to `data/out/rag/<ts>.json`.

Nothing written back to raw data; retrieval is stateless except the
provenance log.

## New code

- `corpus_sync/raggraph.py` — ES loader, Neo4j schema+loader, RAG query
  (stdlib-requests for ES; `neo4j` driver for the graph; both lazily
  imported so the rest of the suite stays dependency-free)
- `corpus_sync/cli.py` — subcommands:
  - `load-es --raw … --snap … [--embed] [--limit N]`
  - `load-graph --raw … --snap … [--limit N] [--related]`
  - `rag "query" [--lang ar|en|fa|auto] [--book B] [--top N] [--no-graph]`
  - `graph-stats` / `es-stats`
- `tests/test_raggraph.py` — offline: constraint DDL generation, batch
  chunking, RRF fusion, citation verification, idempotent MERGE statement
  shape, ES doc assembly with vector. Live smoke on s52 with N=50 docs.

## Phasing / order of operations

1. (now) design doc + module + unit tests, commit, deploy
2. live sample load: first ~50 enriched docs → ES + Neo4j → verify
   counts, constraints, trilingual search, graph traversals
3. wait for Phase 3 full enrichment (+ `--embed`) to finish
4. full load: all 50k docs → ES + Neo4j (bulk, checkpointed)
5. Phase 5 validation: retrieval quality on fixed QA set (AR/EN/FA),
   citation correctness, idempotent re-run, graph relation checks

## Risks / open items

- Neo4j 2025.10.1 is a fresh, never-used store → no migration concerns;
  first load will be the baseline (snapshot of `neo4j_data` before load).
- 50k kNN docs on int8_hnsw in a 30GB-class index: fine (single node).
- Persian full-text quality: standard analyzer only — acceptable for v1;
  a dedicated Persian analyzer (analysis-performant) is a follow-up.
- `RELATED_TO` topic matching is coarse by design; LLM-candidate topics are
  free text — normalization (lowercase, collapse whitespace, strip
  punctuation) is the only join key.
