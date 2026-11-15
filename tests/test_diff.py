# -*- coding: utf-8 -*-
"""Tests for corpus_sync.diff (sentence-level, punctuation-invariant).

Matching semantics (pinned here; supersedes the plan's sketch — see ledger):
- sentences are keyed by NFKC + punctuation-stripped + whitespace-collapsed text;
- exact-key multiset matches cancel (reordering is NOT a change);
- unmatched leftovers are greedily paired when one sentence's content is
  mostly contained in the other (containment >= 0.6) -> each pair = 1 modified;
- remaining new sentences = added, remaining old = removed.
"""
from corpus_sync import diff


def test_punctuation_only_change_is_not_a_change():
    d = diff.sentence_diff("Hello, world.", "Hello world.", "en")
    assert (d.added, d.removed, d.modified) == (0, 0, 0)


def test_added_and_removed_sentences():
    d = diff.sentence_diff("One. Two.", "One. Three. Four.", "en")
    assert (d.added, d.removed, d.modified) == (2, 1, 0)


def test_modified_sentence_detected():
    d = diff.sentence_diff("One. Two.", "One. Two changed.", "en")
    assert d.modified == 1 and d.added == 0 and d.removed == 0


def test_arabic_reordering_not_flagged_as_modified():
    a = "أحمد بن محمد. قال الامام."
    b = "قال الامام. أحمد بن محمد."
    d = diff.sentence_diff(a, b, "ar")
    assert (d.added, d.removed, d.modified) == (0, 0, 0)


def test_arabic_punctuation_insensitive():
    d = diff.sentence_diff("قال أمير المؤمنين. السلام عليكم.",
                           "قال أمير المؤمنين، السلام عليكم.", "ar")
    assert (d.added, d.removed, d.modified) == (0, 0, 0)


def test_modified_pairing_leaves_others_counted():
    # "two" is contained in "two changed" -> modified; "three" is new -> added
    d = diff.sentence_diff("One. Two.", "One. Two changed. Three.", "en")
    assert (d.added, d.removed, d.modified) == (1, 0, 1)


def test_fully_different_sentences_are_added_removed():
    d = diff.sentence_diff("One.", "Two.", "en")
    assert (d.added, d.removed, d.modified) == (1, 1, 0)


def test_empty_and_blank_texts():
    assert diff.sentence_diff("", "", "en") == diff.SentenceDiff(0, 0, 0)
    d = diff.sentence_diff("", "One.", "en")
    assert (d.added, d.removed, d.modified) == (1, 0, 0)
    d = diff.sentence_diff("One.", "", "ar")
    assert (d.added, d.removed, d.modified) == (0, 1, 0)


def test_duplicate_sentences_counted_as_multiset():
    d = diff.sentence_diff("One. One.", "One.", "en")
    assert (d.added, d.removed, d.modified) == (0, 1, 0)
    d = diff.sentence_diff("One.", "One. One.", "en")
    assert (d.added, d.removed, d.modified) == (1, 0, 0)
