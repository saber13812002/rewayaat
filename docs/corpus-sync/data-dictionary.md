# Rewayaat corpus-sync — Data Dictionary

Date: 2026-11-15 (UTC). This dictionary is written **field-for-field against
`corpus_sync/normalize.py`** (v1.0). If the code and this document disagree,
the code wins — and the normalization version must be bumped.

The pipeline has three data layers:

| Layer | Where | Contents |
|---|---|---|
| **Raw** | `data/raw/snapshot-<ts>.jsonl` | One JSON line per fetched doc: the exact API response (`raw`) plus pipeline bookkeeping (`source_id`, `book`, `fetched_at`, `page`, `label`) |
| **Normalized** | SQLite `records` table + `data/out/publish/data/current/normalized.{jsonl,parquet}` | The stable v1.0 schema (this document) |
| **Changes** | SQLite `changes` table + `data/out/publish/changes/<month>.jsonl` | Per-doc status and sentence-level stats between two snapshots |

---

## 1. Raw snapshot line (`data/raw/snapshot-<ts>.jsonl`)

Written by `snapshot._append_page` (corpus_sync/snapshot.py:374).
One line per (page, doc) occurrence — a doc may appear on several lines
(overlapping queries); the finalize pass dedupes by `source_id` keeping the
last occurrence, and the published dataset uses the deduped file.

| Key | Type | Source | Notes |
|---|---|---|---|
| `source_id` | string | `doc["_id"]` | Stable across all sources: `<Book-Name>:<hadithNumber>` |
| `book` | string | query book name | The catalog book name the doc was fetched under |
| `fetched_at` | string | fetch clock | ISO-8601 UTC, e.g. `2026-11-15T21:47:13Z` |
| `page` | int | pagination | 1-based page number of the query that returned the doc |
| `label` | string | query planner | `book:<name>` / `book:<name> chapter:<c>` / `book:<name> number:<n>` |
| `raw` | object | API response | The full enriched doc (25 fields, see §4) verbatim |

---

## 2. Normalized record — schema v1.0

Produced by `normalize_doc(doc, *, book_slug, fetched_at)`
(corpus_sync/normalize.py:70). 16 fields. **Nothing is dropped**: every raw
field not lifted into a top-level field is preserved verbatim inside
`metadata` (§2.3), and the whole raw doc is preserved in `raw`.

### 2.1 Field-by-field

| # | Field | Type | Source | Transform | Example |
|---|---|---|---|---|---|
| 1 | `source_id` | string | `raw._id` | verbatim | `Al-Amali-Saduq:1` |
| 2 | `book_id` | string | query book name | `.lower()` — the pipeline slug | `al-amali-saduq` |
| 3 | `book` | string | `raw.book` | verbatim, `""` if null | `Al-Amali (Saduq)` |
| 4 | `chapter` | string | `raw.chapter` | verbatim, `""` if null | `5` |
| 5 | `hadith_number` | string | `raw.number` | `str(number)` if not None, else `""` | `1` |
| 6 | `narrator` | string | `raw.chain_people` (last entry) | last `chain_people` entry: `t` field if it is a dict, the string itself if a plain string, `""` if empty/absent (corpus_sync/normalize.py:58) | `Muḥammad b. al-Ḥasan al-Ṣaffār` |
| 7 | `arabic` | string | `raw.arabic` | verbatim, `""` if null | (Arabic matn) |
| 8 | `english` | string | `raw.english` | verbatim, `""` if null | (English translation) |
| 9 | `metadata` | object | `raw` | `{k: v for k, v in doc.items() if k not in ("_id","arabic","english")}` — **every** raw field except the three lifted above, verbatim | `{"book_ar":"الأمالى للصدوق", "chain_people":[...], "gradings":[...], ...}` |
| 10 | `fetched_at` | string | fetch clock | ISO-8601 UTC (pipeline bookkeeping, not content) | `2026-11-15T21:47:13Z` |
| 11 | `record_hash` | string | content fields | SHA-256 of canonical JSON over `{source_id, book, chapter, hadith_number, arabic, english, metadata}` (§3) | 64 hex chars |
| 12 | `arabic_hash` | string | `arabic` | `field_hash(arabic)` (§3) | 64 hex chars |
| 13 | `english_hash` | string | `english` | `field_hash(english)` (§3) | 64 hex chars |
| 14 | `metadata_hash` | string | `metadata` | `field_hash(canonical_json(metadata))` (§3) | 64 hex chars |
| 15 | `normalization_version` | string | `config.NORM_VERSION` | constant `"1.0"` | `1.0` |
| 16 | `raw` | object | API response | the full raw doc verbatim | (25-field doc) |

### 2.2 Hash definitions

`field_hash(text)` (corpus_sync/normalize.py:46):

1. `None`/empty → SHA-256 of the empty byte string.
2. Otherwise: **NFKC** Unicode compatibility fold → outer `.strip()` →
   UTF-8 → **SHA-256** hex.
3. Internal whitespace is **not** collapsed (collapsing changes text).

`record_hash` (corpus_sync/normalize.py:85):

- **canonical JSON** = `json.dumps(obj, sort_keys=True, ensure_ascii=False,
  separators=(",",":"))`
- SHA-256 over the 7 content fields only:
  `source_id, book, chapter, hadith_number, arabic, english, metadata`.
- **Excluded on purpose**: `book_id` and `fetched_at` — pipeline
  bookkeeping, not content. Re-fetching an unchanged doc therefore keeps its
  `record_hash`, which is what makes week-over-week diffing cheap (hash
  comparison in SQLite, no raw re-read for unchanged docs).

### 2.3 The `metadata` field (everything else)

For a live by-id doc the 25 API fields are:

`_id, arabic, book, book_ar, chain_links, chain_links_en, chain_people,
chapter, chapter_ar, english, gradings, history, llm_similar, number, part,
part_ar, related, section, section_ar, semantic_english_hint_source,
semantic_matn_source, semantic_significant_terms_source, source, source_ar,
tags, topic_tags, volume`

Lifted out: `_id` → `source_id`, `arabic` → `arabic`, `english` →
`english`. **All other 22 fields land in `metadata` verbatim**, plus `number`
and `book`/`chapter` (lifted AND copied into metadata — the dict
comprehension only excludes the three keys above). No field is dropped;
the reason for the lift is a stable queryable schema, not data loss.

---

## 3. Sentence splitting rules (per language)

`split_sentences(text, lang)` (corpus_sync/normalize.py:106). Used by the
diff (T6) and by `analytics.ensure_sentence_totals` (T8).

| Lang | Regex | Meaning |
|---|---|---|
| `ar` | `(?<=[.!?؟،؛])\s+` or `\n` | Split on whitespace **after** a terminal mark (ASCII `. ! ?` or Arabic `؟ ، ؛`); the mark stays on the **left** sentence. Newlines always split. |
| `en` | `\n` or `(?<=[.!?])\s+` | Same with only ASCII terminals. |

Fragments are stripped; empty fragments are dropped.

---

## 4. Sentence diff semantics (change stats)

`sentence_diff(old, new, lang)` (corpus_sync/diff.py:57), pinned by
`tests/test_diff.py`:

1. Split both texts (§3).
2. Key each sentence: NFKC → drop all Unicode punctuation/symbol characters →
   collapse whitespace. This key is punctuation/spacing-invariant, so
   re-punctuating a sentence is not a change.
3. Cancel exact-key **multiset** matches — reordering sentences is not a
   change (duplicates are matched one-for-one).
4. Greedily pair remaining old/new keys when one mostly contains the other
   (word-overlap ratio ≥ **0.6** in either direction) → each pair counts as
   1 **modified** sentence (an edit, e.g. a word insertion).
5. Leftover new keys = **added**; leftover old keys = **removed**.

Result: `SentenceDiff(added, removed, modified)` per language.

---

## 5. Change detection (`changes` table, `changes/<month>.jsonl`)

Produced by `detect_changes(conn, *, old_snap_id, new_snap_id)`
(corpus_sync/detect.py:93) from two **complete** snapshots.

Per doc, `doc_status`:

| Status | Condition |
|---|---|
| `NEW` | `source_id` in new, not in old |
| `REMOVED` | in old, not in new |
| `UNCHANGED` | in both, `record_hash` identical |
| `MODIFIED` | in both, `record_hash` differs |

For `MODIFIED` docs, `changed_fields` = the subset of
`[arabic, english, metadata, chapter, hadith_number]` whose field hashes
differ; `sentence_stats` = `{"ar": {added, removed, modified}, "en": {...}}`
from §4 (computed only for fields that changed).

Gate: both snapshots must be `status='complete'` in the `snapshots` table,
otherwise `detect.IncompleteSnapshotError` is raised — a partial snapshot is
never silently diffed.

---

## 6. Published dataset layout

`publish.build_dataset_dir` (corpus_sync/publish.py:88) stages:

```
data/out/publish/
  README.md                    dataset card (counts, books, month, license)
  data/current/normalized.jsonl    one line per doc, the 16 v1.0 fields
  data/current/normalized.parquet  same records; nested `metadata`/`raw`
                                   stored as canonical JSON strings
  data/raw/<snap_id>.jsonl       byte-identical copy of the raw snapshot
  changes/<month>.jsonl          one line per NEW/MODIFIED/REMOVED doc
  reports/<month>.md             monthly change report
  manifests/manifest.json        sha256 of every staged file except itself
```

Dataset: `saber13812002/rewayaat-corpus` on Hugging Face, **private**
(`HF_IS_PRIVATE=True`) until a data license is confirmed
(`HF_LICENSE="UNCONFIRMED - private"`).

---

## 7. SQLite store tables (corpus_sync/store.py:25)

| Table | Key | Purpose |
|---|---|---|
| `snapshots` | `id` (`YYYYMMDD-HHMMSS[-seq]`) | one row per run: `status` complete/partial, `total_docs`, `raw_path` (DATA_DIR-relative POSIX — portable across machines), `book_counts` JSON |
| `records` | `(snapshot_id, source_id)` | the v1.0 hashes per doc — doc-level status without re-reading raw |
| `changes` | `(snapshot_id, source_id)` | `doc_status`, `changed_fields` JSON, `sentence_stats` JSON |
| `sentence_totals` | `(snapshot_id, lang)` | cached per-snapshot raw sentence counts (`ar`/`en`), filled idempotently by analytics |

All writes are `INSERT OR REPLACE`; the DB uses WAL.
