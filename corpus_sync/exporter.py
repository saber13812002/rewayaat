# -*- coding: utf-8 -*-
"""Standalone, self-contained corpus export (Phase 2).

Produces a versioned, integrity-hashed, Elasticsearch-reimportable export of a
finalized snapshot. The export is STANDALONE: it is derived from the raw
snapshot JSONL (byte-identical provenance) and never reads or mutates the
pipeline SQLite. Raw data is NEVER replaced by enriched data — enrichment
(Persian / entities / links) is a later, separate pass (Phase 3) that writes
its own files and is keyed on the same ``source_id``.

Layout written under ``dest``::

    dest/
      records.jsonl         one ES-indexable doc per line (_id = source_id)
      records.parquet       same records; nested values as canonical JSON strings
      raw.jsonl             byte-identical copy of the source snapshot
      manifest.json         sha256 of every file + per-book counts + provenance
      data_dictionary.md    field-by-field description (for humans)
      mappings.json         ES index mapping for re-import
      README.md             how to consume + re-import into Elasticsearch

Design notes
------------
- Each line of ``records.jsonl`` is a *complete* ES document. Re-import is a
  plain bulk ``POST {index}/_doc/{source_id}`` per line (see README + the
  Phase-4 importer). ``source_id`` is the stable, corpus-wide unique key
  (e.g. ``Al-Amali-Saduq:1``) and is used verbatim as the ES ``_id``.
- The Arabic matn, English translation, book, bab/chapter, hadith number, and
  narrator are first-class flat fields. The *entire* raw doc plus a verbatim
  ``metadata`` sub-dict are also carried so no upstream field is ever dropped.
- Hashes (record/arabic/english/metadata) are computed by the pinned
  normalizer (``corpus_sync.normalize``), so an export is byte-reproducible
  for a given snapshot and the hashes match the pipeline's stored baseline.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import config, normalize
from .snapshot import _dedupe

EXPORT_VERSION = "1.0"

# Top-level flat columns of the export record (also the parquet schema).
# Nested / list values are serialized to canonical JSON strings in the parquet.
_FLAT_STRING_COLS = (
    "source_id", "book", "book_slug", "chapter", "chapter_ar", "bab",
    "hadith_number", "part", "section", "volume",
    "narrator", "narrator_en", "source", "source_ar",
    "arabic", "english", "persian",
    "arabic_hash", "english_hash", "record_hash", "metadata_hash",
    "fetched_at", "snapshot_id", "normalization_version",
)
# Nested / list columns stored as JSON in parquet, as native values in jsonl.
_NESTED_COLS = (
    "topic_tags", "tags", "gradings", "related", "history",
    "chain", "chain_links", "chain_links_en",
    "semantic_matn", "semantic_terms",
    "provenance", "metadata", "raw", "enrichment",
)
_ALL_COLS = _FLAT_STRING_COLS + _NESTED_COLS


class ExportError(RuntimeError):
    """Raised when the export cannot be built."""


# --- helpers -----------------------------------------------------------------

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _canon_json_str(obj) -> str:
    return normalize.canonical_json(obj)


def _last_chain_name_en(raw: dict) -> str:
    links = raw.get("chain_links_en")
    if isinstance(links, list) and links:
        last = links[-1]
        if isinstance(last, dict) and isinstance(last.get("t"), str):
            return last["t"]
    return ""


def build_export_doc(line: dict, snap_id: str) -> dict:
    """Build one ES-indexable export document from a raw snapshot line.

    ``line`` is a snapshot JSONL line: ``{source_id, book, fetched_at, page,
    label, raw}``. Returns a dict in ``_ALL_COLS`` order.
    """
    raw = line.get("raw") or {}
    book = line.get("book") or raw.get("book") or ""
    book_slug = str(book).lower()
    fetched_at = line.get("fetched_at", "")

    norm = normalize.normalize_doc(raw, book_slug=book_slug, fetched_at=fetched_at)
    arabic = norm["arabic"]
    english = norm["english"]
    chapter = norm["chapter"]
    metadata = norm["metadata"]

    prov = {
        "source": "rewayaat.info /v1/narrations",
        "snapshot_id": snap_id,
        "fetched_at": fetched_at,
        "normalization_version": config.NORM_VERSION,
        "record_hash": norm["record_hash"],
        "arabic_hash": norm["arabic_hash"],
        "english_hash": norm["english_hash"],
        "metadata_hash": norm["metadata_hash"],
    }

    doc = {
        "source_id": line.get("source_id") or raw.get("_id") or "",
        "book": book,
        "book_slug": book_slug,
        "chapter": chapter,
        "chapter_ar": raw.get("chapter_ar") or "",
        "bab": chapter,  # mission terminology: bab == chapter
        "hadith_number": norm["hadith_number"],
        "part": str(raw.get("part") or ""),
        "section": str(raw.get("section") or ""),
        "volume": str(raw.get("volume") or ""),
        "narrator": norm["narrator"],
        "narrator_en": _last_chain_name_en(raw),
        "source": raw.get("source") or "",
        "source_ar": raw.get("source_ar") or "",
        "arabic": arabic,
        "english": english,
        # Phase-3 placeholder: Persian translation is added later under the SAME
        # source_id. Kept in the export so the trilingual shape is explicit.
        "persian": None,
        "arabic_hash": norm["arabic_hash"],
        "english_hash": norm["english_hash"],
        "record_hash": norm["record_hash"],
        "metadata_hash": norm["metadata_hash"],
        "fetched_at": fetched_at,
        "snapshot_id": snap_id,
        "normalization_version": config.NORM_VERSION,
        "topic_tags": raw.get("topic_tags") or [],
        "tags": raw.get("tags") or [],
        "gradings": raw.get("gradings") or [],
        "related": raw.get("related") or [],
        "history": raw.get("history") or [],
        "chain": raw.get("chain_people") or [],
        "chain_links": raw.get("chain_links") or [],
        "chain_links_en": raw.get("chain_links_en") or [],
        "semantic_matn": raw.get("semantic_matn_source") or "",
        "semantic_terms": raw.get("semantic_significant_terms_source") or "",
        "provenance": prov,
        "metadata": metadata,
        "raw": raw,
        # Phase-3 placeholder: filled by the LLM enrichment pass (same
        # source_id) when the export is built with an enrichment.jsonl.
        "enrichment": None,
    }
    return {k: doc[k] for k in _ALL_COLS}


def _book_counts(docs: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for d in docs:
        out[d["book"]] = out.get(d["book"], 0) + 1
    return out


# --- manifest / data dictionary / mapping ------------------------------------

def _data_dictionary() -> str:
    return f"""# Rewayaat Corpus Export — Data Dictionary (v{EXPORT_VERSION})

One record per narration. The stable, corpus-wide unique key is
`source_id` (e.g. `Al-Amali-Saduq:1`); it is also the Elasticsearch `_id`.

## Identity & structure
| Field | Type | Description |
|-------|------|-------------|
| `source_id` | string | **Unique key** = ES `_id`. Format `<BookSlug-Source>:<hadithNumber>`. |
| `book` | string | Book display name (e.g. `Al-Amālī`). |
| `book_slug` | string | Lower-cased book name (pipeline bookkeeping). |
| `chapter` | string | Bab / chapter title (English). |
| `bab` | string | Alias of `chapter` (mission terminology). |
| `chapter_ar` | string | Bab / chapter title (Arabic). |
| `hadith_number` | string | Hadith number within the book. |
| `part` | string | Part (e.g. `Content`). |
| `section` | string | Section / assembly number. |
| `volume` | string | Volume. |
| `narrator` | string | Final narrator in the chain (Arabic name text). |
| `narrator_en` | string | Final narrator (English transliteration), if present. |
| `source` | string | Book's compiling author (English). |
| `source_ar` | string | Book's compiling author (Arabic). |

## Text (the three languages)
| Field | Type | Description |
|-------|------|-------------|
| `arabic` | string | **Arabic original text** (raw matn, unmodified). Authoritative. |
| `english` | string | **English translation** (as published). Not authoritative vs Arabic. |
| `persian` | string/null | **Persian translation** — populated by the Phase-3 LLM pass under the same `source_id`. `null` until enriched. LLM output is reviewable, never authoritative. |

## Tags & references
| Field | Type | Description |
|-------|------|-------------|
| `topic_tags` | list[str] | Upstream topic tags. |
| `tags` | list[str] | Upstream free tags. |
| `gradings` | list | Upstream authenticity gradings. |
| `related` | list[obj] | Upstream external source references (`{{url,title}}`). |
| `history` | list | Upstream editorial history. |

## Narration chain (isnad)
| Field | Type | Description |
|-------|------|-------------|
| `chain` | list[obj] | `chain_people`: `{{p,s,t,e,g,k,o}}` name spans in the Arabic text. |
| `chain_links` | list[obj] | Resolved people links (Arabic names), `{{p,s,t,e,g}}`. |
| `chain_links_en` | list[obj] | Resolved people links (English transliteration). |
| `semantic_matn` | string | Cleaned semantic matn (Arabic), upstream-provided. |
| `semantic_terms` | string | Upstream significant-terms string (Arabic). |

## Provenance & integrity
| Field | Type | Description |
|-------|------|-------------|
| `provenance.source` | string | Upstream endpoint. |
| `provenance.snapshot_id` | string | Snapshot this record was cut from. |
| `provenance.fetched_at` | string | Fetch timestamp (ISO-8601 UTC). |
| `provenance.normalization_version` | string | Normalizer rules version. |
| `provenance.record_hash` | string | SHA-256 of canonical content JSON (id/book/chapter/number/ar/en/metadata). |
| `provenance.arabic_hash` | string | SHA-256 (NFKC+strip) of the Arabic text. |
| `provenance.english_hash` | string | SHA-256 (NFKC+strip) of the English text. |
| `provenance.metadata_hash` | string | SHA-256 (NFKC+strip) of canonical metadata JSON. |
| `record_hash` / `arabic_hash` / `english_hash` / `metadata_hash` | string | Top-level mirrors of the provenance hashes (for cheap columnar checks). |
| `fetched_at` | string | Fetch timestamp. |
| `snapshot_id` | string | Snapshot id. |
| `normalization_version` | string | Normalizer version. |
| `metadata` | obj | **All upstream fields verbatim** (nothing dropped except `_id`/`arabic`/`english` which are promoted). |
| `raw` | obj | The **entire** raw API document, byte-for-byte (provenance). |

## Phase-3 LLM enrichment (separate pass, merged at export time)
| Field | Type | Description |
|-------|------|-------------|
| `enrichment` | obj/null | Full LLM enrichment record under the same `source_id`. `null` until the Phase-3 pass has run and been merged into the export. LLM output is reviewable, never authoritative. |
| `enrichment.persian` | string/null | Persian translation (also mirrored to the top-level `persian` column). |
| `enrichment.entities` | list[obj] | `{{type, value, note}}` — people/places/concepts/objects in the text. |
| `enrichment.topics` | list[str] | 3–8 lowercase English topical tags. |
| `enrichment.quran_references` | list[obj] | `{{surah (1-114), ayah (>=1), ref_type (quote/mention/allusion), span}}`. |
| `enrichment.hadith_candidates` | list[obj] | `{{topic, reason}}` — search suggestions for related narrations (no invented hadith numbers/ids). |
| `enrichment.provenance` | obj | `model`, `prompt_version`, `prompt_hash`, `latency_ms`, `completion_tokens`, `finish_reason`, `validation {{status, issues}}`, `generated_at`. |

## Re-import into Elasticsearch
See `mappings.json` (index mapping) and `README.md` (bulk import recipe).
Each `records.jsonl` line maps 1:1 to an ES doc with `_id = source_id`.
"""


def _es_mapping() -> dict:
    """ES mapping for the re-import index.

    The analyzer set (``arabic_norm`` / ``english_fold`` /
    ``english_fold_search`` + ``strip_hamza`` char filter + ``translit_names``
    synonyms) is copied verbatim from ``scripts/search/v2_mapping.json`` so the
    multilingual index tokenizes exactly like the production search index.
    """
    return {
        "settings": {
            "index": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "analysis": {
                    "char_filter": {
                        "strip_hamza": {
                            "type": "mapping",
                            "mappings": [
                                "ʿ=>", "ʾ=>", "‘=>", "’=>", "`=>", "´=>", "ʼ=>",
                            ],
                        }
                    },
                    "filter": {
                        "translit_names": {
                            "type": "synonym_graph",
                            "synonyms": [
                                "husayn,hussain,husain,hussayn,hussein,husein",
                                "hasan,hassan,hasan",
                                "abdullah,abdallah,abdulla,abdalla",
                                "muhammad,muhammed,mohammad,mohammed",
                                "ghadir,ghadeer,ghadier",
                                "sadiq,sadeeq,saadiq",
                                "jafar,jaffar,jaafar",
                                "ridha,rida,reza,ridah",
                                "baqir,baqer",
                                "kadhim,kazim,kadhem",
                                "askari,askary",
                                "mahdi,mehdi",
                                "fatima,fatimah,fatema",
                                "zahra,zahraa",
                                "abbas,abas",
                                "umar,omar",
                                "uthman,othman",
                                "aisha,ayesha",
                                "zakat,zakah,zakaat",
                                "salat,salah,salaat",
                                "sawm,saum",
                                "hajj,haj",
                                "quran,koran",
                                "sunnah,sunna",
                                "hadith,hadeeth",
                                "imam,imaam",
                                "shia,shiah,shiite",
                                "kufa,koofa",
                                "karbala,kerbala",
                            ],
                        }
                    },
                    "analyzer": {
                        "arabic_norm": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "decimal_digit",
                                       "arabic_normalization"],
                        },
                        "english_fold": {
                            "char_filter": ["strip_hamza"],
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "kstem"],
                        },
                        "english_fold_search": {
                            "char_filter": ["strip_hamza"],
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding",
                                       "translit_names", "kstem"],
                        },
                    },
                },
            }
        },
        "mappings": {
            "properties": {
                "source_id": {"type": "keyword"},
                "book": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text",
                            "analyzer": "english_fold",
                            "search_analyzer": "english_fold_search",
                        }
                    },
                },
                "book_slug": {"type": "keyword"},
                "chapter": {
                    "type": "text",
                    "analyzer": "english_fold",
                    "search_analyzer": "english_fold_search",
                    "fields": {"kw": {"type": "keyword", "ignore_above": 1024}},
                },
                "bab": {"type": "keyword"},
                "chapter_ar": {
                    "type": "text",
                    "analyzer": "arabic_norm",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "hadith_number": {"type": "keyword"},
                "part": {"type": "keyword"},
                "section": {"type": "keyword"},
                "volume": {"type": "keyword"},
                "narrator": {
                    "type": "text",
                    "analyzer": "arabic_norm",
                    "fields": {"kw": {"type": "keyword", "ignore_above": 256}},
                },
                "narrator_en": {
                    "type": "text",
                    "analyzer": "english_fold",
                    "search_analyzer": "english_fold_search",
                    "fields": {"kw": {"type": "keyword", "ignore_above": 256}},
                },
                "source": {"type": "keyword"},
                "source_ar": {"type": "text", "analyzer": "arabic_norm"},
                "arabic": {"type": "text", "analyzer": "arabic_norm"},
                "english": {
                    "type": "text",
                    "analyzer": "english_fold",
                    "search_analyzer": "english_fold_search",
                },
                "persian": {"type": "text"},
                "arabic_hash": {"type": "keyword"},
                "english_hash": {"type": "keyword"},
                "record_hash": {"type": "keyword"},
                "metadata_hash": {"type": "keyword"},
                "fetched_at": {"type": "date"},
                "snapshot_id": {"type": "keyword"},
                "normalization_version": {"type": "keyword"},
                "topic_tags": {"type": "keyword"},
                "tags": {"type": "keyword"},
                "gradings": {"type": "object", "enabled": False},
                "related": {"type": "object", "enabled": False},
                "history": {"type": "object", "enabled": False},
                "chain": {"type": "object", "enabled": False},
                "chain_links": {
                    "type": "nested",
                    "properties": {
                        "p": {"type": "keyword"},
                        "g": {"type": "keyword"},
                        "t": {"type": "text", "analyzer": "arabic_norm",
                              "fields": {"kw": {"type": "keyword",
                                                "ignore_above": 256}}},
                    },
                },
                "chain_links_en": {"type": "object", "enabled": False},
                "semantic_matn": {"type": "text", "analyzer": "arabic_norm"},
                "semantic_terms": {"type": "text", "analyzer": "arabic_norm"},
                "provenance": {
                    "properties": {
                        "source": {"type": "keyword"},
                        "snapshot_id": {"type": "keyword"},
                        "fetched_at": {"type": "date"},
                        "normalization_version": {"type": "keyword"},
                        "record_hash": {"type": "keyword"},
                        "arabic_hash": {"type": "keyword"},
                        "english_hash": {"type": "keyword"},
                        "metadata_hash": {"type": "keyword"},
                    },
                },
                "metadata": {"type": "object", "enabled": False},
                "raw": {"type": "object", "enabled": False},
                "enrichment": {
                    "properties": {
                        "persian": {"type": "text"},
                        "topics": {"type": "keyword"},
                        "quran_references": {
                            "properties": {
                                "surah": {"type": "integer"},
                                "ayah": {"type": "integer"},
                                "ref_type": {"type": "keyword"},
                                "span": {"type": "text",
                                         "analyzer": "arabic_norm"},
                            },
                        },
                        "entities": {"type": "object", "enabled": False},
                        "hadith_candidates": {"type": "object",
                                              "enabled": False},
                        "provenance": {
                            "properties": {
                                "model": {"type": "keyword"},
                                "prompt_version": {"type": "keyword"},
                                "prompt_hash": {"type": "keyword"},
                                "latency_ms": {"type": "integer"},
                                "completion_tokens": {"type": "integer"},
                                "finish_reason": {"type": "keyword"},
                                "validation": {
                                    "properties": {
                                        "status": {"type": "keyword"},
                                        "issues": {"type": "keyword"},
                                    },
                                },
                                "generated_at": {"type": "date"},
                            },
                        },
                    },
                },
            }
        },
    }


def _readme(snap_id: str, status: str, docs: int,
            book_counts: dict[str, int]) -> str:
    """Plain template (NOT an f-string — the embedded python snippets contain
    their own braces); placeholders are substituted via ``str.replace``."""
    books = "\n".join(f"| {b} | {c} |" for b, c in
                      sorted(book_counts.items()))
    tpl = """# Rewayaat Corpus Export — __SNAP__

Self-contained, integrity-hashed export. Status: **__STATUS__**.
Docs: **__DOCS__**. Export version: `__VER__`.

## Per-book counts
| Book | Docs |
|------|------|
__BOOKS__

## Files
- `records.jsonl` — one ES-indexable doc per line (`_id = source_id`).
- `records.parquet` — columnar copy (nested values as canonical JSON strings).
- `raw.jsonl` — byte-identical source snapshot (provenance).
- `manifest.json` — sha256 of every shipped file + counts + provenance.
  The manifest's own integrity: `manifest_sha256` is the SHA-256 of the
  manifest JSON re-serialized with its `manifest_sha256` field set to `""`
  (see the verify recipe below — a file cannot contain its own hash).
- `data_dictionary.md` — field descriptions.
- `mappings.json` — ES index mapping (analyzers identical to the production
  search index, `scripts/search/v2_mapping.json`).
- `README.md` — this file.

## Verify integrity
```bash
python - <<'PY'
import json, hashlib, pathlib
root = pathlib.Path(".")
mtext = (root/"manifest.json").read_text(encoding="utf-8")
m = json.loads(mtext)
for f in m["files"]:
    p = root/f["path"]
    h = hashlib.sha256(p.read_bytes()).hexdigest()
    assert h == f["sha256"], f"hash mismatch: {f['path']}"
# manifest self-hash: sha256 of the manifest with its own sha256 field set to ""
m2 = dict(m); m2["manifest_sha256"] = ""
base = json.dumps(m2, ensure_ascii=False, indent=1).encode("utf-8")
assert m["manifest_sha256"] == hashlib.sha256(base).hexdigest(), "self-hash"
print("all", len(m["files"]), "files verified + manifest self-hash OK")
PY
```

## Re-import into Elasticsearch (bulk)
```bash
# 1) create the index from the shipped mapping
curl -s -X PUT "http://HOST:9200/rewayaat_multilingual_v0001" \\
  -H 'Content-Type: application/json' --data @mappings.json
# 2) bulk-index (each JSONL line is a complete ES doc, _id = source_id)
python - <<'PY'
import json, pathlib, urllib.request
HOST, IDX = "http://HOST:9200", "rewayaat_multilingual_v0001"
buf = []
def flush():
    if not buf: return
    body = ("\\n".join(buf) + "\\n").encode("utf-8")
    req = urllib.request.Request(f"{HOST}/{IDX}/_bulk", data=body,
                                 headers={"Content-Type": "application/x-ndjson"})
    print(urllib.request.urlopen(req).read().decode()[:400])
    buf.clear()
for line in pathlib.Path("records.jsonl").read_text("utf-8").splitlines():
    if not line.strip(): continue
    doc = json.loads(line)
    buf.append(json.dumps({"index": {"_id": doc["source_id"]}}))
    buf.append(json.dumps(doc, ensure_ascii=False))
    if len(buf) >= 4000: flush()
flush()
PY
```
Re-import is **idempotent**: indexing the same `source_id` twice replaces the
doc (same `_id`), so a re-run produces no duplicates.

## Language provenance rule
`arabic` is the authoritative source text. `english` is the published
translation. `persian` is produced by a later LLM enrichment pass (Phase 3)
under the same `source_id` and is *never* authoritative — it is reviewable and
traceable via its own provenance fields. When the export is built with
`--enrich <enrichment.jsonl>`, the full LLM record is attached under
`enrichment` (top-level `persian` filled) and the manifest records which
enrichment file was used and how many docs were enriched.
"""
    return (tpl.replace("__SNAP__", snap_id)
                .replace("__STATUS__", status)
                .replace("__DOCS__", str(docs))
                .replace("__VER__", EXPORT_VERSION)
                .replace("__BOOKS__", books))


# --- main build ---------------------------------------------------------------

def build_export(dest: Path, *, raw_path: Path, snap_id: str,
                 status: str = "unknown",
                 verify_live_books: list | None = None,
                 enrichment_path: Path | None = None) -> dict:
    """Build the full export under ``dest``; returns the manifest dict.

    ``raw_path`` is the finalized snapshot JSONL. ``verify_live_books`` (a list
    of ``fetch.Book``-like objects with ``.name``/``.count``) is optional; when
    given, completeness is checked and recorded in the manifest.
    ``enrichment_path`` (a Phase-3 ``enrichment.jsonl``) is optional; when
    given, the enrichment records are merged into the export docs under the
    same ``source_id`` (fills ``persian`` + attaches ``enrichment``) and the
    manifest records which file was used and how many docs were enriched.
    """
    dest = Path(dest)
    if dest.exists():
        import shutil
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    lines = _read_lines(raw_path)
    docs = [build_export_doc(l, snap_id) for l in _dedupe(lines)]

    enr_by_id: dict[str, dict] = {}
    if enrichment_path is not None:
        from . import enrich
        enr_by_id = enrich.load_enrichment(Path(enrichment_path))
        for d in docs:
            enrich.apply_enrichment(d, enr_by_id.get(d["source_id"]))

    book_counts = _book_counts(docs)

    # --- records.jsonl --------------------------------------------------------
    records_jsonl = dest / "records.jsonl"
    with open(records_jsonl, "w", encoding="utf-8") as fh:
        for d in docs:
            fh.write(json.dumps(d, ensure_ascii=False) + "\n")

    # --- records.parquet (nested as canonical JSON strings) --------------------
    pq_rows = []
    for d in docs:
        r = dict(d)
        for c in _NESTED_COLS:
            v = r.get(c)
            r[c] = _canon_json_str(v) if v is not None else ""
        pq_rows.append(r)
    schema = pa.schema([(c, pa.string()) for c in _ALL_COLS])
    pq.write_table(pa.Table.from_pylist(pq_rows, schema=schema),
                   dest / "records.parquet")

    # --- raw.jsonl (byte-identical copy) --------------------------------------
    import shutil as _sh
    _sh.copyfile(raw_path, dest / "raw.jsonl")

    # --- data dictionary + mapping + readme -----------------------------------
    (dest / "data_dictionary.md").write_text(_data_dictionary(), encoding="utf-8")
    (dest / "mappings.json").write_text(
        json.dumps(_es_mapping(), ensure_ascii=False, indent=1), encoding="utf-8")
    (dest / "README.md").write_text(
        _readme(snap_id, status, len(docs), book_counts), encoding="utf-8")

    # --- manifest (single write; self-hash over the file with its own
    #     "sha256" field set to "" — unambiguous and verifiable) ---------------
    files = []
    for name in ("records.jsonl", "records.parquet", "raw.jsonl",
                 "data_dictionary.md", "mappings.json", "README.md"):
        p = dest / name
        files.append({"path": name, "sha256": _sha256(p),
                      "size": p.stat().st_size,
                      "lines": _count_lines(p)})

    manifest = {
        "export_version": EXPORT_VERSION,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot_id": snap_id,
        "snapshot_status": status,
        "docs_total": len(docs),
        "books": book_counts,
        "enrichment": None if enrichment_path is None else {
            "source": str(enrichment_path),
            "enriched_docs": sum(1 for d in docs if d.get("enrichment")),
        },
        "files": files,
        "manifest_sha256": "",
        "source_raw": str(raw_path),
    }
    if verify_live_books is not None:
        from .snapshot import verify_completeness
        manifest["completeness_check"] = {
            "live_total": sum(b.count for b in verify_live_books),
            "incomplete": verify_completeness(book_counts, verify_live_books),
        }
    # Self-hash: sha256 of the manifest re-serialized with its own
    # manifest_sha256 field set to "" (a file cannot contain its own hash;
    # this scheme is verifiable — see the README verify recipe and
    # tests/test_exporter.py).
    base = json.dumps(manifest, ensure_ascii=False, indent=1)
    manifest["manifest_sha256"] = hashlib.sha256(base.encode("utf-8")).hexdigest()
    (dest / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return manifest


def _read_lines(path: Path) -> list[dict]:
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _count_lines(path: Path) -> int:
    n = 0
    with open(path, "rb") as fh:
        for _ in fh:
            n += 1
    return n
