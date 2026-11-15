# -*- coding: utf-8 -*-
"""Normalization for rewayaat corpus documents (schema v1.0, rules v1).

Rules v1 (deliberately minimal and documented — do not extend without a
normalization-version bump and a re-baseline):

- field_hash: NFKC compatibility fold + outer strip, then SHA-256 (utf-8).
  Whitespace inside the text is NOT collapsed (collapsing changes text).
  ``None`` hashes as the empty string.
- normalize_doc: extracts the stable schema; everything not extracted goes
  into ``metadata`` VERBATIM (no field is dropped without a reason — the raw
  doc is also kept in ``raw``).
- narrator: live API chain entries are dicts ``{p, s, t, e, g, k, o}`` (see
  tests/fixtures/hadith_alamali_1.json), so the narrator is the ``t`` (name
  text) of the last ``chain_people`` entry; plain strings are accepted
  verbatim for forward compatibility.
- record_hash: SHA-256 of canonical JSON over the content fields only
  (source_id, book, chapter, hadith_number, arabic, english, metadata).
  ``book_id``/``fetched_at`` are pipeline bookkeeping, not content, and are
  excluded so re-fetching an unchanged doc keeps its hash.
- split_sentences: terminal punctuation stays on the left sentence
  (lookbehind split); Arabic additionally splits on the Arabic comma ، and
  semicolon ؛ and newlines. Empty fragments are dropped.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata

from . import config

# Arabic: split on whitespace following a terminal mark (ASCII or Arabic ؟) or
# an Arabic comma ، / semicolon ؛ — the mark stays on the left fragment — plus
# unconditional newline splits.
_AR_SPLIT = re.compile(r"(?<=[.!?؟،؛])\s+|\n")
# English: split on newline or whitespace following a terminal mark.
_EN_SPLIT = re.compile(r"\n|(?<=[.!?])\s+")

_CONTENT_FIELDS = (
    "source_id", "book", "chapter", "hadith_number", "arabic", "english", "metadata",
)


def field_hash(text: str | None) -> str:
    """Stable hash of a text field: NFKC + outer strip, SHA-256."""
    if not text:
        return hashlib.sha256(b"").hexdigest()
    return hashlib.sha256(unicodedata.normalize("NFKC", text).strip().encode("utf-8")).hexdigest()


def canonical_json(obj) -> str:
    """Deterministic JSON for hashing: sorted keys, compact, non-ASCII kept."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _narrator_from_chain(chain) -> str:
    if not chain:
        return ""
    last = chain[-1]
    if isinstance(last, str):
        return last
    if isinstance(last, dict):
        t = last.get("t")
        return t if isinstance(t, str) else ""
    return ""


def normalize_doc(doc: dict, *, book_slug: str, fetched_at: str) -> dict:
    """Normalize one raw API doc into the v1.0 record schema."""
    arabic = doc.get("arabic") or ""
    english = doc.get("english") or ""
    chapter = doc.get("chapter") or ""
    metadata = {k: v for k, v in doc.items() if k not in ("_id", "arabic", "english")}
    content = {
        "source_id": doc.get("_id"),
        "book": doc.get("book"),
        "chapter": chapter,
        "hadith_number": str(doc.get("number")) if doc.get("number") is not None else "",
        "arabic": arabic,
        "english": english,
        "metadata": metadata,
    }
    record_hash = hashlib.sha256(canonical_json(content).encode("utf-8")).hexdigest()
    return {
        "source_id": content["source_id"],
        "book_id": book_slug,
        "book": doc.get("book") or "",
        "chapter": chapter,
        "hadith_number": content["hadith_number"],
        "narrator": _narrator_from_chain(doc.get("chain_people")),
        "arabic": arabic,
        "english": english,
        "metadata": metadata,
        "fetched_at": fetched_at,
        "record_hash": record_hash,
        "arabic_hash": field_hash(arabic),
        "english_hash": field_hash(english),
        "metadata_hash": field_hash(canonical_json(metadata)),
        "normalization_version": config.NORM_VERSION,
        "raw": doc,
    }


def split_sentences(text: str, lang: str) -> list[str]:
    """Split text into sentences per the pinned rule for ``lang`` (ar|en)."""
    if not text:
        return []
    pattern = _AR_SPLIT if lang == "ar" else _EN_SPLIT
    return [part for part in (p.strip() for p in pattern.split(text)) if part]
