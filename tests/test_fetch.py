import json

from corpus_sync import fetch

FIXTURES = "tests/fixtures"


def test_books_parses_catalog(monkeypatch):
    raw = open(f"{FIXTURES}/books.json", "rb").read()
    monkeypatch.setattr("corpus_sync.http.get_json", lambda url, params=None: json.loads(raw))
    books = fetch.books()
    # Full list as returned by /v1/browse/books on 2026-11-15 (fixture books.json).
    assert [b.count for b in books] == [
        1469, 1281, 14242, 574, 45, 11012, 1241, 201, 290, 226, 750,
        6379, 829, 1895, 555, 4612, 2260, 49, 1106, 951, 71,
    ]
    assert sum(b.count for b in books) == 50038
    kafi = next(b for b in books if b.slug == "al-kafi")
    assert kafi.name == "Al-Kāfi"
    assert kafi.name_ar == "الكافي"


def test_lookup_maps_response(monkeypatch):
    raw = open(f"{FIXTURES}/lookup_khisal_p1.json", "rb").read()
    monkeypatch.setattr("corpus_sync.http.get_json", lambda url, params=None: json.loads(raw))
    res = fetch.lookup('book:"Al-Khiṣāl"', 1, per_page=3)
    assert res.total == 1281 and len(res.collection) == 3
    assert res.collection[0]["_id"] == "Al-Khisal-Saduq:1282"


def test_lookup_uses_lookup_mode_params(monkeypatch):
    seen = {}

    def cap(url, params=None):
        seen.update(params or {})
        return {"totalResultSetSize": 0, "collection": []}

    monkeypatch.setattr("corpus_sync.http.get_json", cap)
    fetch.lookup('book:"X"', 2, per_page=200)
    assert seen["sort_fields"] == "book,number" and seen["page"] == 2 and seen["per_page"] == 200
    assert seen["q"] == 'book:"X"'


def test_facets_passes_exact_book_name(monkeypatch):
    seen = {}

    def cap(url, params=None):
        seen["url"] = url
        seen.update(params or {})
        return {"book": "Al-Kāfi", "facets": {}, "primaryFacet": "chapter"}

    monkeypatch.setattr("corpus_sync.http.get_json", cap)
    res = fetch.facets("Al-Kāfi")
    assert seen["book"] == "Al-Kāfi"
    assert res["primaryFacet"] == "chapter"


def test_hadith_returns_narration_object(monkeypatch):
    monkeypatch.setattr(
        "corpus_sync.http.get_json",
        lambda url, params=None: {"ok": True, "narration": {"_id": "Al-Khisal-Saduq:1", "book": "Al-Khiṣāl"}},
    )
    doc = fetch.hadith("Al-Khisal-Saduq:1")
    assert doc["_id"] == "Al-Khisal-Saduq:1"
