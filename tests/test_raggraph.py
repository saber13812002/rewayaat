# -*- coding: utf-8 -*-
"""Tests for Phase 4 (RAG + knowledge graph) — corpus_sync.raggraph.

Everything is offline: ES talks to a recording fake ``requests.Session``;
Neo4j talks to fake drivers that record statements and return canned rows;
the LLM is a dict-returning fake client (same contract as
``enrich.LlmClient.chat`` / ``.embed``).

Covered:
- index body = exporter mapping + the production ``semantic_vector``
- bulk NDJSON framing (op line + doc line, id = source_id)
- RRF fusion math + determinism
- ES query bodies (multi_match fields, structured filters, knn body)
- key normalizers (topic / book / chapter / entity / verse)
- graph row builder (narrator s-order, entity/verse/topic dedupe, keys)
- Cypher statements (7 unique constraints, UNWIND-MERGE shapes,
  deterministic RELATED_TO ORDER BY / LIMIT)
- ES loader end-to-end (index created once, idempotent re-run,
  persian filled from enrichment, vectors attached, dedupe, limit)
- graph state save/load/resume + rebuild
- related pass + 1-hop expansion + stats
- language detection, citation text, verify/drop of ⟦…⟧ citations
- full run_rag orchestration (fusion, graph expansion, citation
  verification, provenance log)
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from corpus_sync import config, enrich, exporter, normalize, raggraph


# --- fixtures ------------------------------------------------------------------

def _raw_line(i: int, sid: str | None = None) -> dict:
    return {
        "book": "Al-Kāfi",
        "fetched_at": "2026-11-01T00:00:00Z",
        "label": f"book:Al-Kāfi chapter: Ch {i}",
        "page": 1,
        "source_id": sid or f"Al-Kafi-Saduq:{i}",
        "raw": {
            "_id": sid or f"Al-Kafi-Saduq:{i}",
            "book": "Al-Kāfi",
            "number": i,
            "part": "1",
            "part_ar": "الاوّل",
            "chapter": f"Chapter {i}",
            "section": "",
            "volume": "2",
            "source": "Al-Kāfī",
            "arabic": f"arabic text {i}.",
            "english": f"English text {i}.",
            "chain_links": [
                {"p": "n1", "s": 10, "t": "Ali ibn Ibrahim",
                 "g": "ali-ibn-ibrahim", "e": 16},
                {"p": "n2", "s": 2, "t": "His father",
                 "g": "father", "e": 8},
            ],
            "chain_links_en": [
                {"p": "n2", "s": 2, "t": "Abu al-Hasan",
                 "g": "father", "e": 8},
                {"p": "n1", "s": 10, "t": "Ali ibn Ibrahim",
                 "g": "ali-ibn-ibrahim", "e": 16},
            ],
        },
    }


def _write_raw(path, lines) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for l in lines:
            fh.write(json.dumps(l, ensure_ascii=False) + "\n")


def _enrichment_rec(i: int) -> dict:
    return {
        "source_id": f"Al-Kafi-Saduq:{i}",
        "persian": f"ترجمه فارسی {i}",
        "topics": ["Justice", "justice"],
        "entities": [
            {"type": "person", "value": "Imam Ali", "note": "the fourth caliph"},
            {"type": "", "value": "", "note": "skipped"},
        ],
        "quran_references": [
            {"surah": 2, "ayah": 255, "ref_type": "quotation", "span": "ayat"},
            {"surah": 999, "ayah": 1},
            {"surah": "2", "ayah": 3},
        ],
        "hadith_candidates": [
            {"topic": "Faith", "reason": "r"},
            {"topic": "Faith", "reason": "dup"},
        ],
        "provenance": {"model": "gpt-oss-120b", "prompt_version": "1.0",
                       "validation": {"status": "pass", "issues": []},
                       "generated_at": "2026-11-01T00:00:00Z"},
    }


def _write_enrichment(path) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for i in (1, 2):
            fh.write(json.dumps(_enrichment_rec(i), ensure_ascii=False) + "\n")


def _write_vectors(path) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"source_id": "Al-Kafi-Saduq:1",
                             "vector": [0.1, 0.2, 0.3]}) + "\n")


# --- ES mapping ----------------------------------------------------------------

def test_vector_mapping_matches_production():
    m = raggraph.vector_mapping()
    v2 = json.loads((Path(__file__).resolve().parent.parent
                     / "scripts" / "search" / "v2_mapping.json"
                     ).read_text(encoding="utf-8"))
    assert m == v2["mappings"]["properties"]["semantic_vector"]


def test_es_index_body_extends_exporter_mapping():
    before = json.dumps(exporter._es_mapping(), sort_keys=True,
                        ensure_ascii=False)
    body = raggraph.es_index_body()
    after = json.dumps(exporter._es_mapping(), sort_keys=True,
                       ensure_ascii=False)
    assert before == after, "es_index_body must not mutate the exporter mapping"
    props = body["mappings"]["properties"]
    assert props["semantic_vector"] == raggraph.vector_mapping()
    for col in ("source_id", "arabic", "english", "persian", "enrichment",
                "chain_links"):
        assert col in props, col
    assert body["settings"]["index"]["analysis"]["analyzer"]["arabic_norm"]


def test_bulk_lines_op_and_doc_pairs():
    lines = raggraph._bulk_lines(
        [{"source_id": "A", "v": 1}, {"source_id": "B", "v": 2}], "idx")
    assert len(lines) == 4
    assert json.loads(lines[0]) == {"index": {"_index": "idx", "_id": "A"}}
    assert json.loads(lines[1]) == {"source_id": "A", "v": 1}
    assert json.loads(lines[2]) == {"index": {"_index": "idx", "_id": "B"}}
    assert json.loads(lines[3]) == {"source_id": "B", "v": 2}


# --- ES fake session ---------------------------------------------------------------

class _FakeResp:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body, ensure_ascii=False)

    def json(self):
        return self._body


class _FakeESSession:
    """Records (method, url, kwargs); serves canned ES responses from a store."""

    def __init__(self, store: dict):
        self.store = store
        self.calls = []

    def _route(self, method: str, url: str, kwargs: dict):
        self.calls.append((method, url, kwargs))
        if method == "PUT":
            self.store["index_exists"] = True
            self.store["index_body"] = kwargs.get("json")
            return _FakeResp(200, {"acknowledged": True})
        if method == "POST" and url.endswith("/_bulk"):
            lines = kwargs["data"].strip().splitlines()
            out = []
            for i in range(0, len(lines), 2):
                op = json.loads(lines[i])
                doc = json.loads(lines[i + 1])
                assert op["index"]["_index"] == self.store["index"]
                self.store["docs"][op["index"]["_id"]] = doc
                out.append({"index": {"result": "created",
                                      "_id": op["index"]["_id"]}})
            return _FakeResp(200, {"items": out})
        if method == "POST" and url.endswith("/_search"):
            key = json.dumps(kwargs.get("json"), sort_keys=True)
            return _FakeResp(200, self.store["search_results"].get(
                key, {"hits": {"hits": []}}))
        if method == "POST" and url.endswith("/_mget"):
            ids = kwargs["json"]["ids"]
            return _FakeResp(200, {"docs": [
                {"_id": i, "found": i in self.store["docs"],
                 "_source": self.store["docs"].get(i)} for i in ids]})
        if method == "GET" and url.endswith("/_count"):
            return _FakeResp(200, {"count": len(self.store["docs"])})
        if method == "GET" and url.rstrip("/").endswith("/" + self.store["index"]):
            if self.store.get("index_exists"):
                return _FakeResp(200, {"indices": {
                    self.store["index"]: {"primaries": 3,
                                          "status": "green"}},
                    "status": "green"})
            return _FakeResp(404, {"error": "index_not_found_exception"})
        if method == "GET":
            return _FakeResp(404, {"error": "index_not_found_exception"})
        raise AssertionError(f"unhandled {method} {url}")

    def get(self, url, **kwargs):
        return self._route("GET", url, kwargs)

    def post(self, url, **kwargs):
        return self._route("POST", url, kwargs)

    def request(self, method, url, **kwargs):
        return self._route(method, url, kwargs)


def _es(fake: _FakeESSession) -> raggraph.EsClient:
    return raggraph.EsClient(base_url="http://es", session=fake)


# --- query bodies -----------------------------------------------------------------

def test_build_query_body_trilingual_fields():
    body = raggraph.build_query_body("justice", top_k=5)
    mm = body["query"]["multi_match"]
    assert set(mm["fields"]) == {"arabic", "english", "persian", "chapter",
                                 "book", "narrator_en", "chapter_ar", "bab"}
    assert mm["type"] == "best_fields"
    assert mm["query"] == "justice"
    assert body["size"] == 5


def test_build_query_body_structured_filters():
    body = raggraph.build_query_body("x", filters={
        "book": "Al-Kāfi", "hadith": "7", "volume": "2"}, top_k=3)
    must = body["query"]["bool"]["must"]
    assert {"term": {"book": "Al-Kāfi"}} in must
    assert {"term": {"hadith_number": "7"}} in must
    assert {"term": {"volume": "2"}} in must
    # no filters -> plain multi_match, no bool wrapper
    assert "bool" not in raggraph.build_query_body("y")["query"]


def test_knn_body_shape_and_filter():
    body = raggraph.knn_body([0.1, 0.2], top_k=10)
    knn = body["knn"]
    assert knn["field"] == "semantic_vector"
    assert knn["k"] == 10 and knn["num_candidates"] == 50
    assert body["size"] == 10
    b2 = raggraph.knn_body([0.1], filters={"book": "B"}, top_k=5)
    assert b2["query"]["bool"]["filter"] == [{"term": {"book": "B"}}]
    assert b2["knn"]["num_candidates"] == 50  # max(20, 50)


def test_rrf_fuse_math_and_determinism():
    fused = raggraph.rrf_fuse([["a", "b", "c"], ["b", "a", "c"]], k=60)
    # a (rank 1+2) and b (rank 2+1) are an exact tie -> id order breaks it
    assert fused[0] == ("a", 1 / 61 + 1 / 62)
    assert fused[1] == ("b", 1 / 62 + 1 / 61)
    assert fused[2] == ("c", 2 / 63)
    assert fused[0][1] == fused[1][1] > fused[2][1]
    # empty entries skipped
    assert raggraph.rrf_fuse([["x", ""], ["x"]])[0][0] == "x"
    # exact tie -> deterministic id order
    assert [s for s, _ in raggraph.rrf_fuse([["b", "a"], ["a", "b"]])] \
        == ["a", "b"]
    # single list keeps its rank order
    assert [s for s, _ in raggraph.rrf_fuse([["b", "a"]])] == ["b", "a"]


# --- key normalizers ------------------------------------------------------------------

def test_normalizers():
    assert raggraph.normalize_topic("  Justice & Faith ") == "justice faith"
    assert raggraph.normalize_topic("العدل") == "العدل"
    assert raggraph.book_key("Al-Kāfi", "2") == "Al-Kāfi|2"
    assert raggraph.chapter_key("B", "2", "1", "Ch", "Sec") == "B|2|1|Ch|Sec"
    assert raggraph.entity_key("person", " Imam  Ali ") == "person:imam ali"
    assert raggraph.entity_key("", "X") == "concept:x"
    assert raggraph.verse_key(2, 255) == "2:255"


# --- graph rows -----------------------------------------------------------------------

def test_iter_graph_rows_narrator_s_order_and_keys():
    line = _raw_line(1)
    rows = raggraph.iter_graph_rows(line, None, "snap-T")
    narr = rows["narrator"]
    assert [n["narrator_id"] for n in narr] == ["n2", "n1"]
    assert [n["position"] for n in narr] == [1, 2]
    assert narr[0]["node"] == {"name_ar": "His father",
                               "name_en": "Abu al-Hasan", "slug": "father"}
    assert narr[1]["node"]["name_en"] == "Ali ibn Ibrahim"
    assert rows["narrated_by"] == [
        {"sid": "Al-Kafi-Saduq:1", "narrator_id": "n2", "position": 1},
        {"sid": "Al-Kafi-Saduq:1", "narrator_id": "n1", "position": 2},
    ]
    n = rows["narration"][0]
    assert n["node"]["hadith_number"] == "1"
    assert n["node"]["enriched"] == 0 and n["node"]["persian"] == ""
    ref = normalize.normalize_doc(
        line["raw"], book_slug="al-kāfi", fetched_at=line["fetched_at"])
    assert n["node"]["record_hash"] == ref["record_hash"]
    # shared-node rows carry their unique keys
    assert rows["book"][0] == {"book_key": "Al-Kāfi|2",
                               "node": {"book": "Al-Kāfi", "volume": "2",
                                        "source": "Al-Kāfī"}}
    assert rows["chapter"][0]["chapter_key"] == "Al-Kāfi|2|1|Chapter 1|"
    assert rows["chapter"][0]["node"]["part_ar"] == "الاوّل"
    # relationship rows
    assert rows["in_book"] == [{"sid": "Al-Kafi-Saduq:1",
                                "book_key": "Al-Kāfi|2"}]
    assert rows["has_chapter"] == [{"book_key": "Al-Kāfi|2",
                                    "chapter_key": "Al-Kāfi|2|1|Chapter 1|"}]
    # no enrichment -> no entity/topic/verse/candidate rows
    assert rows["entity"] == [] and rows["topic"] == []
    assert rows["verse"] == [] and rows["references"] == []
    assert rows["candidate_topic"] == []


def test_iter_graph_rows_enrichment():
    line = _raw_line(1)
    enr = {
        "source_id": "Al-Kafi-Saduq:1",
        "persian": "ترجمه",
        "topics": ["Justice", "justice", ""],
        "entities": [{"type": "person", "value": "Imam Ali"},
                     {"value": "no-type"}],
        "quran_references": [{"surah": 2, "ayah": 255},
                             {"surah": 999, "ayah": 1},
                             {"surah": "2", "ayah": 3},
                             {"surah": 1, "ayah": 0}],
        "hadith_candidates": [{"topic": "Faith"}, {"topic": "Faith"}],
        "provenance": {"validation": {"status": "pass"}},
    }
    rows = raggraph.iter_graph_rows(line, enr, "snap-T")
    assert rows["narration"][0]["node"]["persian"] == "ترجمه"
    assert rows["narration"][0]["node"]["enriched"] == 1
    assert rows["narration"][0]["node"]["validation_status"] == "pass"
    # topic dedupe via normalize; candidate topics also become Topic nodes
    assert [t["value"] for t in rows["topic"]] == ["justice", "faith"]
    assert rows["has_topic"] == [{"sid": "Al-Kafi-Saduq:1", "value": "justice"}]
    # entity keys (missing type -> concept; dash normalizes to space)
    assert [e["entity_key"] for e in rows["entity"]] == [
        "person:imam ali", "concept:no type"]
    assert [m["entity_key"] for m in rows["mentions_entity"]] == [
        "person:imam ali", "concept:no type"]
    # verse validation (int, 1..114, ayah>=1)
    assert [v["verse_key"] for v in rows["verse"]] == ["2:255"]
    assert rows["references"][0]["ref_type"] == "mention"
    # candidate dedupe
    assert [c["value"] for c in rows["candidate_topic"]] == ["faith"]


# --- Cypher statements --------------------------------------------------------------------

def test_constraints_cover_all_merge_keys():
    assert len(raggraph.CONSTRAINTS) == 7
    joined = " ".join(raggraph.CONSTRAINTS)
    for key in ("n.source_id", "n.narrator_id", "b.book_key",
                "c.chapter_key", "v.verse_key", "t.value", "e.entity_key"):
        assert key in joined, key
    assert joined.count("CREATE CONSTRAINT IF NOT EXISTS") == 7


def test_statements_shape():
    for st, kind in raggraph._STATEMENT_ORDER:
        assert st.lstrip().startswith("UNWIND $rows AS r"), kind
        assert "MERGE" in st, kind
    assert raggraph._STATEMENT_ORDER[0][1] == "narration"
    # deterministic related pass
    assert "WHERE b.source_id <> $sid" in raggraph.ST_RELATED
    assert "ORDER BY shared DESC, b.source_id ASC" in raggraph.ST_RELATED
    assert "LIMIT $top" in raggraph.ST_RELATED
    assert "r.via = 'topics'" in raggraph.ST_RELATED
    assert len(raggraph.GRAPH_LABELS) == 7
    assert len(raggraph.GRAPH_RELS) == 9


# --- ES loader ------------------------------------------------------------------------------

def test_load_es_end_to_end_and_idempotent(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    lines = [_raw_line(1), _raw_line(2), _raw_line(1)]  # duplicate sid
    _write_raw(raw, lines)
    edir = config.OUT_DIR / "enrich" / "snap-T"
    edir.mkdir(parents=True, exist_ok=True)
    _write_enrichment(edir / enrich.OUTPUT_NAME)
    _write_vectors(edir / enrich.EMBED_NAME)

    store = {"index": "idx", "docs": {}, "search_results": {}}
    fake = _FakeESSession(store)
    es = _es(fake)
    res = raggraph.load_es(raw, "snap-T", es=es, index="idx")

    assert res["total"] == 2 and res["indexed"] == 2 and res["errors"] == 0
    assert res["no_vector"] == 1 and res["enriched"] == 2
    assert res["vectors"] == 1 and res["index"] == "idx"
    # index created exactly once; doc id = source_id
    puts = [c for c in fake.calls if c[0] == "PUT"]
    assert len(puts) == 1
    assert len(store["docs"]) == 2
    d1 = store["docs"]["Al-Kafi-Saduq:1"]
    assert d1["persian"] == "ترجمه فارسی 1"
    assert d1["enrichment"]["topics"] == ["Justice", "justice"]
    assert d1["semantic_vector"] == [0.1, 0.2, 0.3]
    assert d1["source_id"] == "Al-Kafi-Saduq:1"
    assert store["docs"]["Al-Kafi-Saduq:2"]["persian"] == "ترجمه فارسی 2"

    # re-run: index exists -> no new PUT, same docs
    res2 = raggraph.load_es(raw, "snap-T", es=es, index="idx")
    assert res2["indexed"] == 2 and res2["errors"] == 0
    assert len([c for c in fake.calls if c[0] == "PUT"]) == 1

    with pytest.raises(FileNotFoundError):
        raggraph.load_es(tmp_path / "nope.jsonl", "snap-T", es=es)


def test_load_es_limit(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [_raw_line(i) for i in (1, 2, 3)])
    store = {"index": "idx", "docs": {}, "search_results": {}}
    fake = _FakeESSession(store)
    es = _es(fake)
    res = raggraph.load_es(raw, "snap-T", es=es, index="idx", limit=2)
    assert res["total"] == 2 and res["indexed"] == 2
    assert len(store["docs"]) == 2
    assert "Al-Kafi-Saduq:3" not in store["docs"]


def test_es_stats_shape(tmp_data_dir):
    store = {"index": "idx", "docs": {"a": {}}, "search_results": {},
             "index_exists": True}
    fake = _FakeESSession(store)
    stats = raggraph.es_stats(_es(fake), index="idx")
    assert stats == {"index": "idx", "count": 1, "shards": 3,
                     "health": "green"}


# --- graph loader / state ----------------------------------------------------------------------

class _FakeCounters:
    def __init__(self, created=0):
        self.relationships_created = created


class _FakeResult:
    def __init__(self, data=None, created=0):
        self._data = data or []
        self.counters = _FakeCounters(created)

    def data(self):
        return self._data

    def single(self):
        return self._data[0]

    def consume(self):
        return self


class _FakeSession:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, statement, **params):
        """Record the call; return a _FakeResult (driver API: .data(),
        .single(), .consume())."""
        self.driver.calls.append((statement, params))
        items = list(self.driver.responder(statement, params))
        if items and isinstance(items[0], _FakeResult):
            return items[0]
        return _FakeResult(data=items)


class _FakeDriver:
    def __init__(self, responder=None):
        self.calls = []
        self.responder = responder or (lambda s, p: [])
        self.closed = False

    def session(self):
        return _FakeSession(self)

    def close(self):
        self.closed = True


def test_load_graph_state_resume_and_rebuild(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [_raw_line(1), _raw_line(2)])
    out_dir = tmp_path / "graph" / "snap-T"

    # fresh load
    driver = _FakeDriver()
    res = raggraph.load_graph(driver, raw, {}, snap_id="snap-T",
                              out_dir=out_dir)
    assert res["loaded"] == 2 and res["skipped_completed"] == 0
    assert res["total"] == 2 and res["snap_id"] == "snap-T"
    state = json.loads(
        (out_dir / raggraph.GRAPH_STATE_NAME).read_text(encoding="utf-8"))
    assert state["snap_id"] == "snap-T"
    assert state["completed"] == ["Al-Kafi-Saduq:1", "Al-Kafi-Saduq:2"]
    assert "updated_at" in state
    # schema constraints ran
    constraints = [c for c in driver.calls if c[0].startswith(
        "CREATE CONSTRAINT")]
    assert len(constraints) == len(raggraph.CONSTRAINTS)
    # node-creation rows land first in the batch (narration, then book)
    unwinds = [st for st, _ in driver.calls
               if st.lstrip().startswith("UNWIND")]
    assert unwinds[0] == raggraph._STATEMENT_ORDER[0][0]
    assert unwinds[1] == raggraph._STATEMENT_ORDER[1][0]  # book rows exist

    # resume: everything already done
    driver2 = _FakeDriver()
    res2 = raggraph.load_graph(driver2, raw, {}, snap_id="snap-T",
                               out_dir=out_dir)
    assert res2["loaded"] == 0 and res2["skipped_completed"] == 2
    assert not [c for c in driver2.calls if c[0].lstrip().startswith("UNWIND")]

    # different snap_id -> fresh load
    driver3 = _FakeDriver()
    res3 = raggraph.load_graph(driver3, raw, {}, snap_id="snap-OTHER",
                               out_dir=out_dir)
    assert res3["loaded"] == 2 and res3["skipped_completed"] == 0

    # rebuild clears the graph first
    driver4 = _FakeDriver()
    raggraph.load_graph(driver4, raw, {}, snap_id="snap-T", out_dir=out_dir,
                        rebuild=True)
    assert any(c[0] == "MATCH (n) DETACH DELETE n" for c in driver4.calls)

    # missing raw file
    with pytest.raises(FileNotFoundError):
        raggraph.load_graph(driver, tmp_path / "nope.jsonl", {},
                            snap_id="snap-T", out_dir=out_dir)


def test_load_graph_chunks_and_state_flush(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [_raw_line(i) for i in (1, 2, 3)])
    out_dir = tmp_path / "g" / "s"
    driver = _FakeDriver()
    res = raggraph.load_graph(driver, raw, {}, snap_id="s", out_dir=out_dir,
                              batch=2)
    assert res["loaded"] == 3
    # batch=2 over 3 lines -> ST_NARRATION rows flush in chunks of 2 then 1
    narration_rows = [p for st, p in driver.calls
                      if st == raggraph.ST_NARRATION]
    assert [len(p["rows"]) for p in narration_rows] == [2, 1]
    state = json.loads(
        (out_dir / raggraph.GRAPH_STATE_NAME).read_text(encoding="utf-8"))
    assert state["completed"] == [
        "Al-Kafi-Saduq:1", "Al-Kafi-Saduq:2", "Al-Kafi-Saduq:3"]


# --- related pass + expansion ---------------------------------------------------------------------

def test_run_related_pass_counts_created():
    def responder(statement, params):
        if statement == raggraph.ST_RELATED:
            yield _FakeResult(created=3)
        elif "MATCH (n:Narration) RETURN" in statement:
            yield _FakeResult(data=[{"sid": "a"}, {"sid": "b"}])

    driver = _FakeDriver(responder)
    res = raggraph.run_related_pass(driver)
    assert res["narrations"] == 2 and res["related_created"] == 6
    assert res["top"] == raggraph.RELATED_TOP
    assert res["min_shared"] == raggraph.RELATED_MIN_SHARED
    rel_calls = [p for st, p in driver.calls if st == raggraph.ST_RELATED]
    assert [p["sid"] for p in rel_calls] == ["a", "b"]
    # explicit sids list
    driver2 = _FakeDriver(responder)
    res2 = raggraph.run_related_pass(driver2, sids=["x"], top=4, min_shared=3)
    assert res2["narrations"] == 1 and res2["related_created"] == 3
    assert res2["top"] == 4 and res2["min_shared"] == 3


def test_expand_graph_four_rels():
    def responder(statement, params):
        if "NARRATED_BY" in statement:
            yield _FakeResult(data=[{"sid": "a", "items": ["n1", None]}])
        if "HAS_TOPIC" in statement:
            yield _FakeResult(data=[{"sid": "a", "items": ["justice"]}])
        if "REFERENCES" in statement:
            yield _FakeResult(data=[{"sid": "a", "items": ["2:255"]}])
        if "RELATED_TO" in statement:
            yield _FakeResult(data=[{"sid": "a", "items": ["b"]}])

    driver = _FakeDriver(responder)
    out = raggraph.expand_graph(driver, ["a"])
    assert out["a"] == {"narrators": ["n1"], "topics": ["justice"],
                        "verses": ["2:255"], "related": ["b"]}
    assert raggraph.expand_graph(driver, []) == {}


def test_graph_stats_queries_all_labels_and_rels():
    driver = _FakeDriver(lambda s, p: [_FakeResult(data=[{"c": 7}])])
    stats = raggraph.graph_stats(driver)
    assert set(stats["labels"]) == set(raggraph.GRAPH_LABELS)
    assert set(stats["relationships"]) == set(raggraph.GRAPH_RELS)
    assert all(v == 7 for v in stats["labels"].values())
    assert all(v == 7 for v in stats["relationships"].values())


# --- RAG language + citations -----------------------------------------------------------------------

def test_detect_lang():
    assert raggraph.detect_lang("What is the meaning of tawbah?") == "en"
    assert raggraph.detect_lang("ما هو التوبة؟") == "ar"
    assert raggraph.detect_lang("توبه چیست؟") == "fa"
    assert raggraph.detect_lang("no script here") == "en"
    assert raggraph.detect_lang("") == "en"


def _ctx_doc(i: int) -> dict:
    return {"source_id": f"S:{i}", "book": "Al-Kāfi", "volume": "2",
            "part": "1", "chapter": f"Chapter {i}", "section": "",
            "hadith_number": str(i), "arabic": f"ar {i}",
            "english": f"en {i}", "persian": f"fa {i}"}


def test_citation_text_and_verify():
    c1, c2 = _ctx_doc(1), _ctx_doc(2)
    text = raggraph.citation_text(c1)
    assert text == "Al-Kāfi · vol. 2 · part 1 · Chapter 1 · hadith 1"
    ans = f"claim ⟦{text}⟧ other ⟦Al-Kāfi · vol. 9⟧"
    cit = raggraph.verify_citations(ans, [c1, c2])
    assert cit["verified_count"] == 1 and cit["total"] == 2
    assert len(cit["unverified"]) == 1
    assert raggraph.verify_citations("no citations", [c1])["total"] == 0
    dropped = raggraph.drop_unverified_citations(ans, [c1, c2])
    assert f"⟦{text}⟧" in dropped
    assert "⟦Al-Kāfi · vol. 9⟧" not in dropped


def test_build_rag_prompt_blocks():
    msgs = raggraph.build_rag_prompt("Q?", "en", [_ctx_doc(1)])
    assert msgs[0]["role"] == "system" and "⟦…⟧" in msgs[0]["content"]
    user = msgs[1]["content"]
    assert "QUESTION (en):\nQ?" in user
    assert "⟦Al-Kāfi · vol. 2 · part 1 · Chapter 1 · hadith 1⟧ (S:1)" in user
    assert "AR: ar 1" in user and "EN: en 1" in user and "FA: fa 1" in user


# --- run_rag orchestration ------------------------------------------------------------------------

class _FakeLlmClient:
    def __init__(self, answer: str):
        self.answer = answer
        self.chat_calls = []

    def chat(self, messages):
        self.chat_calls.append(messages)
        return {"content": self.answer, "finish_reason": "stop",
                "completion_tokens": 12, "latency_ms": 340,
                "model": "gpt-oss-120b", "budget": 4096}

    def embed(self, texts, model=None):
        return [[0.1, 0.2, 0.3] for _ in texts]


def _rag_es_store() -> dict:
    store = {"index": "idx", "search_results": {},
             "docs": {f"S:{i}": _ctx_doc(i) for i in (1, 2, 3, 4)}}
    # text search (query "q", top 20) -> S:1, S:2 ; kNN -> S:2, S:3
    store["search_results"][
        json.dumps(raggraph.build_query_body("q", top_k=20), sort_keys=True)
    ] = {"hits": {"hits": [{"_id": "S:1"}, {"_id": "S:2"}]}}
    store["search_results"][
        json.dumps(raggraph.knn_body([0.1, 0.2, 0.3], top_k=20), sort_keys=True)
    ] = {"hits": {"hits": [{"_id": "S:2"}, {"_id": "S:3"}]}}
    return store


def test_run_rag_full_path(tmp_data_dir):
    store = _rag_es_store()
    fake = _FakeESSession(store)
    es = _es(fake)
    c1 = _ctx_doc(1)
    answer = (f"answer ⟦{raggraph.citation_text(c1)}⟧ "
              f"and a bad one ⟦Unknown · hadith 99⟧")
    llm = _FakeLlmClient(answer)

    def responder(statement, params):
        if "RELATED_TO" in statement:
            yield _FakeResult(data=[{"sid": "S:2", "items": ["S:4"]}])
        # other expansion rels -> no rows

    driver = _FakeDriver(responder)
    out_dir = config.OUT_DIR / "rag"
    result = raggraph.run_rag(
        "q", es=es, driver=driver, client=llm, index="idx", out_dir=out_dir)

    # language + retrieval
    assert result["lang"] == "en"
    assert result["retrieval"]["text_hits"] == 2
    assert result["retrieval"]["knn_hits"] == 2
    fused = result["retrieval"]["fused"]
    assert fused[0] == "S:2"  # appears in both lists
    assert set(fused) == {"S:1", "S:2", "S:3"}
    # graph expansion: RELATED_TO neighbor S:4 fetched as an extra doc
    assert result["retrieval"]["graph_expansion"]["S:2"]["related"] == ["S:4"]
    assert result["retrieval"]["extra_docs"] == ["S:4"]
    # citations: 1 verified, 1 unverified -> dropped, partial status
    cit = result["citations"]
    assert cit["verified_count"] == 1 and cit["total"] == 2
    assert cit["status"] == "partial_citation"
    assert "⟦Unknown · hadith 99⟧" not in result["answer"]
    assert f"⟦{raggraph.citation_text(c1)}⟧" in result["answer"]
    # provenance
    prov = result["provenance"]
    assert prov["model"] == "gpt-oss-120b"
    assert prov["prompt_version"] == raggraph.RAG_PROMPT_VERSION
    assert len(prov["prompt_hash"]) == 64
    assert prov["finish_reason"] == "stop" and prov["budget"] == 4096
    # context entries carry origin + citation
    ctx = result["context"]
    assert all(c["origin"] in ("retrieval", "graph") for c in ctx)
    assert any(c["origin"] == "graph" for c in ctx)
    assert ctx[0]["citation"].startswith("⟦")
    # provenance log written
    log = json.loads(Path(result["provenance_log"]).read_text("utf-8"))
    assert log["query"] == "q"
    assert (out_dir / Path(result["provenance_log"]).name).is_file()


def test_run_rag_all_citations_verified(tmp_data_dir):
    store = _rag_es_store()
    fake = _FakeESSession(store)
    es = _es(fake)
    c2 = _ctx_doc(2)
    llm = _FakeLlmClient(f"answer ⟦{raggraph.citation_text(c2)}⟧")
    result = raggraph.run_rag("q", es=es, driver=None, client=llm,
                              index="idx", out_dir=config.OUT_DIR / "rag")
    assert result["citations"]["status"] == "verified"
    assert result["answer"] == llm.answer


def test_run_rag_no_llm_no_graph(tmp_data_dir):
    store = _rag_es_store()
    fake = _FakeESSession(store)
    es = _es(fake)
    result = raggraph.run_rag("q", es=es, driver=None, client=None,
                              index="idx", out_dir=config.OUT_DIR / "rag",
                              use_graph=False)
    assert result["answer"] is None and result["citations"] is None
    assert result["provenance"] is None
    assert result["retrieval"]["text_hits"] == 2
    assert result["retrieval"]["knn_hits"] == 0
    assert result["retrieval"]["graph_expansion"] is None
    assert result["retrieval"]["extra_docs"] == []
    assert (config.OUT_DIR / "rag" /
            Path(result["provenance_log"]).name).is_file()


def test_run_rag_filters_go_into_query(tmp_data_dir):
    store = _rag_es_store()
    body = raggraph.build_query_body("q", filters={"book": "Al-Kāfi"}, top_k=5)
    store["search_results"][
        json.dumps(body, sort_keys=True)
    ] = {"hits": {"hits": [{"_id": "S:1"}]}}
    fake = _FakeESSession(store)
    es = _es(fake)
    result = raggraph.run_rag("q", es=es, client=None, index="idx",
                              book="Al-Kāfi", top=5,
                              out_dir=config.OUT_DIR / "rag")
    assert result["retrieval"]["text_hits"] == 1
    assert result["retrieval"]["fused"] == ["S:1"]


# --- mget order -------------------------------------------------------------------------------------

def test_mget_preserves_order_and_skips_missing():
    store = _rag_es_store()
    fake = _FakeESSession(store)
    es = _es(fake)
    docs = raggraph._mget_docs(es, "idx", ["S:2", "missing", "S:1"])
    assert [d["source_id"] for d in docs] == ["S:2", "S:1"]
