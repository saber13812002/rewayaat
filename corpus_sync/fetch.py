"""Thin fetch layer over the rewayaat.info REST API.

Mapping only — no business logic. Endpoint behavior is verified in
docs/corpus-sync/01-source-discovery.md (Phase 1).

Key API facts (verified live 2026-11-15):
- GET /v1/browse/books -> list of {name, nameAr, slug, count} (21 books).
- GET /v1/browse/facets?book=<exact name> -> {book, facets, primaryFacet}.
- GET /v1/narrations with non-empty sort_fields = LOOKUP mode:
  {baseResultSetSize, collection, topicTagFacets, totalResultSetSize};
  `collection` items are full 31-field documents.
- GET /v1/narrations/{id} -> {ok, narration}.
"""

from dataclasses import dataclass, field

from . import config, http


@dataclass(frozen=True)
class Book:
    name: str        # exact diacritized name, e.g. "Al-Kāfi"
    name_ar: str
    slug: str
    count: int


@dataclass(frozen=True)
class LookupResult:
    total: int                      # totalResultSetSize (clamped at 10,000!)
    collection: list[dict] = field(default_factory=list)


def books() -> list[Book]:
    """Catalog of all books with live doc counts (GET /v1/browse/books)."""
    data = http.get_json(f"{config.BASE_URL}/v1/browse/books")
    return [Book(name=b["name"], name_ar=b["nameAr"], slug=b["slug"], count=int(b["count"])) for b in data]


def facets(book_name: str) -> dict:
    """Facets for one book (GET /v1/browse/facets?book=<exact name>).

    Returns the parsed JSON: {"book", "facets": {"chapter": [{"name","count"}], ...}, "primaryFacet"}.
    NOTE: very slow in production for large books (observed >35 min for Al-Kāfi once);
    the http layer's 240 s timeout + retries apply.
    """
    return http.get_json(f"{config.BASE_URL}/v1/browse/facets", params={"book": book_name})


def lookup(query: str, page: int, per_page: int = config.PER_PAGE) -> LookupResult:
    """One LOOKUP-mode page (GET /v1/narrations with sort_fields set).

    `query` uses query_string syntax with field scopes, e.g.
    'book:"Al-Kāfi" chapter:"..."'. page is 1-based.
    """
    resp = http.get_json(
        f"{config.BASE_URL}/v1/narrations",
        params={
            "q": query,
            "sort_fields": "book,number",
            "page": page,
            "per_page": per_page,
        },
    )
    return LookupResult(
        total=int(resp.get("totalResultSetSize", 0)),
        collection=list(resp.get("collection", [])),
    )


def hadith(source_id: str) -> dict:
    """Fetch one narration by id (GET /v1/narrations/{id}) -> the narration dict."""
    resp = http.get_json(f"{config.BASE_URL}/v1/narrations/{source_id}")
    return resp["narration"]
