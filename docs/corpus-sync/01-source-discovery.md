# Phase 1 — Source Discovery Report

Date: 2026-11-15 (UTC). All claims verified by live probes; raw outputs in `tmp/`.

## 1. Source inventory

| Source | What it is | Format | Verified content |
|---|---|---|---|
| GitHub repo `rewayaat/rewayaat` (fork: `saber13812002/rewayaat`) | Spring Boot app + data pipeline scripts + `batches/` corpus dumps | Java, YAML, JSONL | HEAD `7f7e96f` (2026-11-13), in sync with upstream `master` |
| `batches/` (163 JSONL files) | ES export 2026-02-03 from index `rewayaat_updated` | JSONL `{_id,_source}` | 32,519 docs, 18 books, 5 fields only (`book`,`number`,`chapter`,`arabic`,`english`) |
| `batches/v1-restored/` (88 JSONL files) | S3 snapshot 2025-02-20, index `rewayaat` (ES 7.10.2) | JSONL `{_id,_source}` | 17,519 docs, 3 books (Ghurar 11,012 / Mīzān 4,612 / Mishkāt 1,895), ~15+ fields (incl. `book_ar`,`chapter_ar`,`source`,`publisher`,`volume`,`part`) |
| Live REST API `https://rewayaat.info` | Production app (DO k8s) | JSON | 14 OpenAPI paths (`/v3/api-docs`); full docs via `GET /v1/narrations/{id}` |
| Live sitemap `/sitemap-hadith-{page}.xml` | Public ID enumeration (PIT + search_after) | XML | PAGE_SIZE=10,000; endpoint live but cold scan can exceed 180s (probe in progress) |
| Elasticsearch (production) | `elasticsearch-v2.elastic-v2.svc.cluster.local`, index `rewayaat_updated` | ES JSON | Per `k8s/deployment.yaml` + `docs/data-pipeline.md`; not directly reachable from local machine |

## 2. Document counts (verified, not from docs)

| Count | Value | Evidence |
|---|---|---|
| Local `batches/` | 32,519 docs, 32,519 unique ids, 0 dupes | `tmp/local_counts.txt` (line-by-line count) |
| Local `batches/v1-restored/` | 17,519 docs | `tmp/v1_counts.txt` |
| Overlap between the two sets | **0** | `tmp/v1_counts.txt` (id set intersection) |
| Local union | **50,038** unique docs, 21 books | `tmp/v1_counts.txt`, `tmp/book_compare.txt` |
| Live API total (`/v1/browse/books` sum) | **50,038** docs, 21 books | `tmp/browse_books.json`, `tmp/book_compare.txt` |
| Per-book match live vs local | **21/21 OK, zero diffs** | `tmp/book_compare.txt` (full table) |

Conclusion: **local corpus and production are in count-parity per book (50,038).** The docs claim "50,038 hadith" is confirmed by live counting on both sides.

## 3. Stable identifiers

- `_id` = `<Book-Name>:<hadithNumber>` (e.g. `Al-Amali-Saduq:1`, `Al-Kafi-Volume-1-Kulayni:1048`).
- Stable across sources: identical in `batches/`, `v1-restored/`, API by-id response, and (by construction) sitemap URLs.
- No `updated_at`/`created_at` field in any source → change detection must use **full-snapshot diffing** (per spec).

## 4. API capability map (live-verified)

| Endpoint | Capability | Limit discovered |
|---|---|---|
| `GET /v1/narrations?q=...&page=&per_page=` | SEARCH mode (relevance) | Hard cap 10,000 hits (`max_result_window`): page 51 → **HTTP 500** (`tmp/api_probe2.txt`) |
| `GET /v1/narrations?sort_fields=book,number&q=...` | LOOKUP mode (deterministic order, paginated) — **returns FULL enriched docs (31 fields) in `collection`** | 10,000 offset cap per query; **sub-queries per book/chapter/volume stay under the cap** |
| `GET /v1/narrations/{id}` | Full enriched doc, **25 fields** incl. `arabic`,`english`,`book`,`book_ar`,`chapter`,`chapter_ar`,`narrator`-related (`chain_links`,`chain_people`),`tags`,`topic_tags`,`gradings`,`history`,`source`,... | Single id per call; no batch endpoint |
| `GET /v1/narrations/page_for_id?id=&q=` | Page number of a doc within top-10k scan | Only within 10k scan window (`found:false` outside it) |
| `GET /v1/browse/books` | Book catalog with live counts | None; 21 books returned |
| `GET /v1/browse/page`, `/v1/browse/facets` | Browse paging | Presumably same 10k window |
| `GET /v1/narrators`, `/v1/narrators/{id}`, `/v1/terms/*` | Narrator/term corpora | Separate indexes (`rewayaat_narrators`) |

**25 by-id fields** (probe `Al-Amali-Saduq:1`, `tmp/api_probe2.txt`):
`_id, arabic, book, book_ar, chain_links, chain_links_en, chain_people, chapter, chapter_ar, english, gradings, history, llm_similar, number, part, part_ar, related, section, section_ar, semantic_english_hint_source, semantic_matn_source, semantic_significant_terms_source, source, source_ar, tags, topic_tags, volume`

## 5. Production corpus field coverage gap (GitHub batches vs live API)

| Field class | `batches/` | v1-restored | Live by-id |
|---|---|---|---|
| `arabic` | ✅ | ✅ | ✅ |
| `english` | ✅ | ✅ | ✅ |
| `book`,`number`,`chapter` | ✅ | ✅ | ✅ |
| `book_ar`,`chapter_ar`,`source`,`volume`,`part`,... | ❌ | ✅ | ✅ |
| `chain_*`,`tags`,`topic_tags`,`gradings`,`history`,... | ❌ | ❌ | ✅ |

→ GitHub batches are a **stale, 5-field subset** (Feb 2026 export predates most enrichment). The live by-id API is the only source with the full current schema.

## 6. Elasticsearch access

- Production ES lives in the DO k8s cluster (`k8s-c1`, ns `rewayaat-v2`), service `elasticsearch-v2.elastic-v2.svc.cluster.local:9200`, index `rewayaat_updated` — **not reachable from the local machine** (cluster-internal).
- Server 133 = SSH alias `s52` → `172.30.0.133` (hostname `AI-TOOLS`). SSH works. Findings:
  - Docker container `taghrir-es` (ES 8.13.4) exists but is **Exited** — port 9200 not listening.
  - No `/mnt/share`, no rewayaat-specific ES data dirs found on 133.
  - No rewayaat ES credentials found in agent memory (`~/.openclaw/workspace`, `~/.claude/projects`).
- **Status: BLOCKED — awaiting user decision** (direct ES extraction vs API-only extraction).

## 7. Licensing

| Item | License | Evidence |
|---|---|---|
| Code (repo) | Apache 2.0 | `LICENSE` |
| **Corpus data** | **NONE FOUND** — searched `docs/`, `README.md`, templates for CC/CC-BY/data-license: 0 matches | grep results, 2026-11-15 |

→ **Hugging Face dataset must remain PRIVATE until a data license is confirmed.** (Absolute rule from mission.)

## 8. Verified extraction strategy (API-only, per user decision 2026-11-15)

**Key verified facts (live probes, `tmp/lookup_probe4.txt`, `tmp/split_probe.txt`, `tmp/split_local.txt`):**

1. LOOKUP mode (`sort_fields=book,number`) returns **full 31-field enriched docs** in `collection`
   (incl. `arabic`,`english`,`book_ar`,`chapter_ar`,`chain_*`,`tags`,`topic_tags`,`gradings`,`history`,`source`,...).
   No per-id fetch pass is needed.
2. `totalResultSetSize` is reliable per scoped query (verified: Al-Khiṣāl → 1,281 exact; Al-Kāfi → clamped at 10,000).
3. The two >10k books split cleanly by chapter:
   - Al-Kāfi: 2,612 distinct chapters, max chapter = **322 docs**, 0 empty chapters.
   - Ghurar al-Ḥikam: 718 distinct chapters (all vol "1"), max chapter = **389 docs**, 0 empty chapters.
   → every sub-query stays ≤ 389 hits, far under the 10,000 window.
4. Book list + per-book counts from `/v1/browse/books` (21 books, 50,038 total) is the snapshot integrity check.
5. `/v1/browse/facets?book=X` returns chapter names+counts (verified on Ghurar; Al-Kāfi facets call is slow but works).

**Algorithm (full snapshot):**
```
books = GET /v1/browse/books                      # 21 books, expected total T
for book in books:
    total = LOOKUP count for book
    if total <= 10000:
        queries = [book:"<name>"]
    else:
        facets = GET /v1/browse/facets?book=<name>
        queries = [book:"<name>" chapter:"<ch>" for ch in facets.chapters]
    for q in queries:
        for page in 1..ceil(count/200):
            GET /v1/narrations?q=...&sort_fields=book,number&page=<p>&per_page=200
            append collection docs to raw JSONL
integrity: unique(_id) == T and per-book counts match /v1/browse/books
```
Estimated time: ~450–900 requests at 1–4 s each ≈ **1.5–2.5 h** for the first full snapshot;
syncs reuse the same path (cheap enough for weekly cadence).

**Robustness requirements (from observed live behavior):**
- Per-request timeout ≥ 240 s; retry with exponential backoff + jitter (observed: same query 1.2 s → >120 s; DNS `getaddrinfo failed` mid-run).
- Checkpoint after every completed page (append to temp file + `page_state.json`), resume on restart (idempotency via `_id` dedupe).
- A snapshot is **complete** only when the per-book totals match `/v1/browse/books`; a doc absent from a partial page set is NOT a deletion (mission rule).

**Kept as fallback:** direct ES scroll (ChainExportTool pattern) if server-133 ES is ever started; `batches/` + `batches/v1-restored/` remain the immutable pre-2026-02 raw archive.

## 9. Open questions / blockers

- [ ] Server-133 ES: user to decide (start `taghrir-es` / provide host+creds / skip direct ES).
- [ ] Sitemap warm-cache behavior (probe pending — non-blocking).
- [ ] Data license for HF publication (keep private meanwhile).
- [ ] Per-book >10k split strategy needs verification (chapter field granularity) before Phase 2 — or use ES.
