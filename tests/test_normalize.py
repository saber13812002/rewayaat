# -*- coding: utf-8 -*-
"""Tests for corpus_sync.normalize (schema v1.0, per-field hashes, rules v1)."""
import json

from corpus_sync import normalize


def test_field_hash_stable_and_nfc_trimmed():
    assert normalize.field_hash("salam") == normalize.field_hash("salam")
    assert normalize.field_hash(None) == normalize.field_hash("")
    assert normalize.field_hash(" a b ") == normalize.field_hash("a b")
    # NFKC compatibility folding (ligature fi -> "fi") is part of rules v1
    assert normalize.field_hash("\ufb01le") == normalize.field_hash("file")


def test_normalize_doc_full_schema():
    doc = json.load(open("tests/fixtures/hadith_alamali_1.json", encoding="utf-8"))
    rec = normalize.normalize_doc(doc, book_slug="al-amali", fetched_at="2026-11-15T00:00:00Z")
    assert rec["source_id"] == "Al-Amali-Saduq:1"
    assert rec["book_id"] == "al-amali"
    assert rec["normalization_version"] == "1.0"
    assert set(rec) == {
        "source_id", "book_id", "book", "chapter", "hadith_number", "narrator",
        "arabic", "english", "metadata", "fetched_at", "record_hash", "arabic_hash",
        "english_hash", "metadata_hash", "normalization_version", "raw",
    }
    assert rec["raw"]["_id"] == "Al-Amali-Saduq:1"
    assert rec["record_hash"] != rec["arabic_hash"]
    # metadata keeps everything except _id / arabic / english, verbatim
    assert "_id" not in rec["metadata"]
    assert "arabic" not in rec["metadata"] and "english" not in rec["metadata"]
    assert rec["metadata"]["chain_people"] == doc["chain_people"]
    # live API chain entries are dicts -> narrator = last entry's name text
    assert rec["narrator"] == doc["chain_people"][-1]["t"]
    assert rec["hadith_number"] == "1"


def test_normalize_doc_minimal_and_empty_fields():
    doc = {"_id": "B:2", "book": "B", "number": "2"}
    rec = normalize.normalize_doc(doc, book_slug="b", fetched_at="t")
    assert rec["chapter"] == "" and rec["arabic"] == "" and rec["english"] == ""
    assert rec["narrator"] == "" and rec["hadith_number"] == "2"
    assert rec["metadata"] == {"book": "B", "number": "2"}


def test_record_hash_changes_with_any_content_change():
    base = {"_id": "B:1", "book": "B", "chapter": "C", "number": "1",
            "arabic": "نص", "english": "text"}
    r1 = normalize.normalize_doc(base, book_slug="b", fetched_at="t")
    r2 = normalize.normalize_doc({**base, "english": base["english"] + "."},
                                 book_slug="b", fetched_at="t")
    assert r1["record_hash"] != r2["record_hash"]
    assert r1["english_hash"] != r2["english_hash"]
    # fetched_at is NOT content: re-fetch of an unchanged doc keeps its hash
    r3 = normalize.normalize_doc(base, book_slug="b", fetched_at="other")
    assert r1["record_hash"] == r3["record_hash"]


def test_split_sentences_arabic_pinned():
    # Pinned per the rule implemented in normalize._AR_SPLIT:
    # (?<=[.!?؟،؛])\s+|\n — terminal punctuation (incl. Arabic ، ؛) stays on
    # the left sentence (lookbehind split); newlines split unconditionally.
    assert normalize.split_sentences("أ. ب، ج؛ د. هـ", "ar") == [
        "أ.", "ب،", "ج؛", "د.", "هـ",
    ]


def test_split_sentences_english_pinned():
    # Pinned per spec regex \n|(?<=[.!?])\s+ : punctuation stays with its
    # sentence; newlines split unconditionally.
    assert normalize.split_sentences("One. Two? Three!\nFour", "en") == [
        "One.", "Two?", "Three!", "Four",
    ]
    assert normalize.split_sentences("", "en") == []
    assert normalize.split_sentences("   ", "ar") == []
