# -*- coding: utf-8 -*-
"""Sentence-level diffing for hadith texts (Arabic + English).

Semantics (pinned by tests/test_diff.py; supersedes the plan's
SequenceMatcher sketch — see SDD ledger):

1. Split both texts with ``normalize.split_sentences``.
2. Key each sentence: NFKC, strip all Unicode punctuation/symbols,
   collapse whitespace -> punctuation- and spacing-invariant identity.
3. Cancel exact-key multiset matches: reordering a doc's sentences is NOT
   a change.
4. Greedily pair unmatched old/new keys when one is mostly contained in
   the other (word-overlap ratio >= 0.6, either direction) -> each pair
   counts as 1 modified sentence (an edit, e.g. a word insertion).
5. Leftover new keys = added, leftover old keys = removed.
"""
from __future__ import annotations

import re
import unicodedata
from collections import Counter
from dataclasses import dataclass

from .normalize import split_sentences

# A sentence counts as "modified" (not added+removed) when at least this
# fraction of its words survives in its counterpart.
_CONTAINMENT_THRESHOLD = 0.6


@dataclass(frozen=True)
class SentenceDiff:
    added: int
    removed: int
    modified: int


def _key(sentence: str) -> str:
    """Punctuation/spacing-invariant identity of a sentence."""
    s = unicodedata.normalize("NFKC", sentence)
    s = "".join(ch for ch in s
                if not unicodedata.category(ch).startswith(("P", "S")))
    return re.sub(r"\s+", " ", s).strip()


def _contain_ratio(a: str, b: str) -> float:
    """Fraction of a's words (multiset) present in b."""
    wa = a.split()
    wb = b.split()
    if not wa or not wb:
        return 0.0
    ca, cb = Counter(wa), Counter(wb)
    inter = sum(min(ca[w], cb[w]) for w in ca)
    return inter / len(wa)


def sentence_diff(old_text: str, new_text: str, lang: str) -> SentenceDiff:
    old_keys = [k for s in split_sentences(old_text, lang) if (k := _key(s))]
    new_keys = [k for s in split_sentences(new_text, lang) if (k := _key(s))]

    # 3) cancel exact matches (multiset)
    remaining_new = Counter(new_keys)
    old_left: list[str] = []
    for k in old_keys:
        if remaining_new.get(k, 0) > 0:
            remaining_new[k] -= 1
        else:
            old_left.append(k)
    new_left = [k for k, n in remaining_new.items() for _ in range(n)]

    # 4) containment pairing, best ratio first
    candidates = []
    for i, a in enumerate(old_left):
        for j, b in enumerate(new_left):
            ratio = max(_contain_ratio(a, b), _contain_ratio(b, a))
            if ratio >= _CONTAINMENT_THRESHOLD:
                candidates.append((ratio, i, j))
    candidates.sort(key=lambda t: (-t[0], t[1], t[2]))
    used_old: set[int] = set()
    used_new: set[int] = set()
    modified = 0
    for _ratio, i, j in candidates:
        if i in used_old or j in used_new:
            continue
        used_old.add(i)
        used_new.add(j)
        modified += 1

    return SentenceDiff(
        added=len(new_left) - len(used_new),
        removed=len(old_left) - len(used_old),
        modified=modified,
    )
