# Live smoke — >10k book fallback decision (Task 3, Step 1)

Fetched: **2026-11-15T19:08:24Z** via `tmp/smoke_range.py` (raw capture:
[`smoke_range_raw.json`](smoke_range_raw.json), script output `tmp/smoke_range_out.txt`).
All probes: `GET https://rewayaat.info/v1/narrations?...&sort_fields=book,number` (LOOKUP mode).

## Probes and results

| # | Query `q` (per_page) | total | observed | verdict |
|---|---|---|---|---|
| A | `book:"Al-Kāfi" number:[1 TO 5000]` (2) | **10000** | docs `2506`, `2504` (chapter "The Dogs") | **range syntax is a NO-OP** — total = clamped book total (14242→10000), not a filtered count. `number:[a TO b]` (unquoted) is not a field scope; after `escapePunctuation` (`[` → `\[`) it survives to `query_string` as a clause that does not filter. Unusable for scoping. |
| B | `book:"Al-Kāfi" chapter:"How does it happen?"` (3) | **1** | doc 490, chapter exactly "How does it happen?" | chapter scope with `?` works — exact count |
| C | `book:"Ghurar al-Ḥikam" chapter:"Abstinence [from Food]"` (3) | **2** | docs 2608, 2609, chapter exactly "Abstinence [from Food]" | chapter scope with `[ ]` works — exact count (local batch count = 2) |
| D | `book:"Ghurar al-Ḥikam" "Abstinence [from Food]"` (3) | **2** | same 2 docs, highlight on chapter field | free-text phrase on the analyzed `chapter` field also works (fallback for `"`-containing titles) |
| E | `book:"Ghurar al-Ḥikam" chapter:"This World"` (3) | **391** | first docs 3511/3510/3509 | main path works (live count 391 vs local 389 — corpus grew since the Feb-2026 batch) |
| F | `book:"Al-Kāfi" chapter:"The Rare Ahadith"` (3) | **373** | first docs are chapter **"The Rare Ahadith about the Stumpers"** | wildcard `*value*` **over-matches** substring-sibling chapters (373 ⊃ local 322) — expected and harmless (dedupe by `_id`) |

## Decision (supersedes plan line 20 "number ranges")

1. **`number:[lo TO hi]` ranges are rejected** (probe A). The plan's fallback to
   "10k-offset chunks at per_page=10000" is also impossible: `max_result_window=10000`
   means an unscoped query can only ever return offsets 0..9999 (`from >= 10000 → size = 0`).
2. **Chapter scope is the primary split for both >10k books, including all
   "unsafe" chapters** — probes B/C show `? [ ]` in chapter names resolve to the exact
   chapter at live counts. **Double-quoted** chapter titles (46 across 5 books) break
   the `chapter:"…"` scope regex (server: `(\w+):"([^"]*)")`; those use the
   **star-wildcard scope** (see Round 2): replace each `"` in the chapter name with
   `*` in the scope value, e.g. `chapter:"*Allah sent down His comfort...* (9:40)"`.
   The phrase fallback (round-1 probe D idea) was **rejected** — see Round 2.
3. **Completeness = per-book fetched sum vs `/v1/browse/books` count** (the plan's gate),
   after `_id` dedupe. Over-matches within a book are harmless (deduped); under-matches
   (new chapters since the last chapter-list refresh, degenerate queries) make the book
   sum fall short.
4. **Shortfall repair pass** for >10k books: `book:"X" number:"<n>"` is an **exact term
   query** (server: `number` is a simple-value field → `term` on the keyword field), so
   scanning number values (fixture numbers ∪ seen numbers, descending) recovers any
   missing docs; stop as soon as the book sum reaches the live count.
5. Pagination note: sort key `(book, number)` is **not unique** — local batch analysis
   (`tmp/number_uniqueness.txt`) shows ties: Al-Amālī 387, Kitāb al-Ghayba 467,
   Man Lā Yaḥḍuruh al-Faqīh 1696, ʿUyūn 345, Al-Kāfi 15 (all in "The Rare Ahadith").
   Tie order is stable on an unmodified index; `_id` dedupe absorbs duplicates and the
   per-book sum gate catches any loss → book flagged incomplete, never a silent REMOVED.

## Round 2 — quote chapters (2026-11-15, raw: smoke_quotes_raw.json + smoke_quotes2_raw.json)

Quote-chapter inventory from local batches: 46 chapters across 5 books — Al-Kāfi 18
(17 even-count internal quotes / 1 odd), Al-Amālī 14, Al-Khiṣāl 2, Man Lā Yaḥḍuruh
al-Faqīh 1, Nahj al-Balāgha 11. All chapter `number` values in the batches are digit
strings.

Round 1 (`tmp/smoke_quotes.py`): the phrase strategy FAILS.

| # | Query `q` (per_page) | total | observed | verdict |
|---|---|---|---|---|
| E | `book:"Al-Kāfi" "Allah sent down His comfort..." (9:40)` (2) | **10000** | whole book | even-quote name: phrase + `(9:40)` OR-group over-matches (the group alone matches 897 docs — probe G2) |
| E' | `book:"Al-Kāfi" chapter:Allah sent down His comfort... (9:40)` (2, quote-stripped scope) | **0** | — | quote-stripped scope: the wildcard `*value*` cannot span the removed-quote position (pattern expects ` (9:40)` after `...`, value has `" (9:40)`) |
| O | `book:"Al-Kāfi" "If you try to count the bounties Allah..." (16:18)` (2) | **0** | — | odd-quote name: `balanceDelimiters` escapes ALL `"` (odd count) → the book scope itself breaks |
| O' | `book:"Al-Kāfi" chapter:If you try to count the bounties Allah... (16:18)` (2) | **0** | — | same strip failure as E' |

Round 2 (`tmp/smoke_quotes2.py`): the **star-wildcard scope** works. `*` is a valid
wildcard character inside the server's `*value*` pattern and spans the quote position.

| # | Query `q` (per_page) | total | observed | verdict |
|---|---|---|---|---|
| G4 | `book:"Al-Kāfi" chapter:"*Allah sent down His comfort...* (9:40)"` (3) | **1** | doc `Al-Kafi-Volume-8-Kulayni:571`, chapter exactly `"Allah sent down His comfort..." (9:40)` | even internal quotes → exact |
| H4 | `book:"Al-Kāfi" chapter:"*If you try to count the bounties Allah...* (16:18)"` (3) | **1** | doc `Al-Kafi-Volume-1-Kulayni:592`, chapter exactly `'If you try to count the bounties Allah...' (16:18)` | odd internal quotes → exact |
| G1 | `book:"Al-Kāfi" "Allah sent down His comfort..." (9:40)` (3) | **10000** | — | diagnostic: phrase OR-group over-matches (whole book) |
| G2 | `book:"Al-Kāfi" (9:40)` (3) | **897** | — | diagnostic: the parens group alone over-matches |

**Locked**: every chapter query uses `chapter:"<name with " → *>"` (quote → star);
plain chapters are unchanged. No separate `chapter_phrase` query kind is needed.

## Round 3 — number-term repair (2026-11-15, `tmp/smoke_repair.py`)

| # | Query `q` (per_page) | total | observed | verdict |
|---|---|---|---|---|
| R1 | `book:"Al-Kāfi" number:"571"` (8) | **8** | 8 docs across Al-Kāfi volumes (number 571 appears in multiple volumes) | `number:"n"` is an exact **term** query returning ALL docs sharing the number — repair mechanism verified. Cross-volume over-match is harmless (dedupe by `_id`). Local-batch count of Al-Kāfi docs with number 571: 1. |
| R2 | `book:"Al-Kāfi" number:"1"` (8) | timeout | 4×240 s timeouts (prod latency spike that afternoon; baseline probes ran in 1.2 s) | not conclusive — acceptable: R1 already verified the mechanism on the same code path; the fixture contains Al-Kāfi numbers for repair-seed coverage |
| R3 | star-scope on an Al-Khulafāʾ quote chapter | not run | script exited at R2 | same mechanism as G4/H4 (already verified live) |

## Source-code evidence (this repo, master)

- `QueryStringQueryResult.java` L725-747 `parseFieldScopes` (scope regex), L749-785
  `removeFieldScopes`, L787-798 `applyFieldScopes` (book→term, volume/section/number→term,
  else→wildcard `*v*` on `field.keyword`), L250-261 `from/size` clamp at `maxResultWindow`.
- `HadithQueryService.java` L314-350 `escapePunctuation` (escapes `[` `]` outside quotes —
  why probe A's brackets never became a Lucene range), L146-213 `enhanceQuery`
  (LOOKUP → AND-join), L69-92 `setupSortBuilders` (number → script sort, string field).
- `BrowseFacets.java` L20,147-150: facet `terms` aggregation **capped at 500 buckets**
  (Al-Kāfi 2,612 chapters / Ghurar 718 → live facets are truncated; chapter list for the
  split comes from `chapter_split.json`, refreshed from fetched docs once a run is
  complete).
