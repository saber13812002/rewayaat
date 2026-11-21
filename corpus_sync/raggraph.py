# -*- coding: utf-8 -*-
"""Phase 4: RAG + knowledge graph over the exported corpus.

Design: docs/corpus-sync/04-phase4-rag-knowledge-graph.md.

Two EXISTING backends on s52:

- **Elasticsearch 8.13** (``http://127.0.0.1:9200``): trilingual
  full-text + kNN index (``rewayaat_corpus_v1`` by default). Docs are
  ``exporter.build_export_doc(line)`` + ``enrich.apply_enrichment`` + the
  Phase-3 ``semantic_vector``; **doc id = ``source_id``**, so re-running
  the loader is idempotent (bulk op ``index`` overwrites the same doc).
- **Neo4j 2025** (``bolt://127.0.0.1:7687``): 7 labels / 9 relationship
  types, all written with idempotent ``MERGE`` batches (``UNWIND $rows``).

LLM output is never authoritative: ``RELATED_TO`` links come from a
deterministic topic-overlap second pass (no LLM), and every RAG answer
carries verified ``⟦…⟧`` citations plus a provenance log under
``DATA_DIR/out/rag/``.

Heavy drivers stay lazy: ``neo4j`` is imported inside ``open_neo4j``; the
ES path uses ``requests`` (already a core dependency). The rest of the
test suite therefore stays dependency-free.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

from . import config, enrich, exporter, normalize

# --- tunables -------------------------------------------------------------------

RRF_K: int = 60                 # reciprocal-rank fusion constant
BULK_BATCH: int = 200           # docs per ES _bulk call
GRAPH_BATCH: int = 1000         # raw lines per Neo4j MERGE batch
RELATED_TOP: int = 5            # RELATED_TO neighbors kept per narration
RELATED_MIN_SHARED: int = 2     # minimum exact shared topics for RELATED_TO
RELATED_SIDS_PER_QUERY: int = 50  # RELATED_TO is one statement per narration
RAG_PROMPT_VERSION: str = "1.0"  # bump when the RAG prompt text changes
ES_REQUEST_TIMEOUT_S: int = 60
ES_BULK_TIMEOUT_S: int = 300

GRAPH_STATE_NAME = "graph_state.json"

#: unique constraints (design doc §Neo4j schema) — created with IF NOT EXISTS
CONSTRAINTS: tuple[str, ...] = (
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Narration) "
    "REQUIRE n.source_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Narrator) "
    "REQUIRE n.narrator_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Book) "
    "REQUIRE b.book_key IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter) "
    "REQUIRE c.chapter_key IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (v:QuranVerse) "
    "REQUIRE v.verse_key IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Topic) "
    "REQUIRE t.value IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) "
    "REQUIRE e.entity_key IS UNIQUE",
)

#: UNWIND-MERGE statements, one per row kind. ``$rows`` is a list of dicts.
ST_NARRATION = """
UNWIND $rows AS r
MERGE (n:Narration {source_id: r.sid})
SET n += r.node
MERGE (n)-[:IN_BOOK]->(b:Book {book_key: r.book_key})
SET b += r.book
MERGE (n)-[:IN_CHAPTER]->(c:Chapter {chapter_key: r.chapter_key})
SET c += r.chapter
MERGE (b)-[:HAS_CHAPTER]->(c)
"""
ST_NARRATOR = """
UNWIND $rows AS r
MATCH (x:Narration {source_id: r.sid})
MERGE (p:Narrator {narrator_id: r.narrator_id})
SET p += r.node
MERGE (x)-[:NARRATED_BY {position: r.position}]->(p)
"""
ST_ENTITY = """
UNWIND $rows AS r
MATCH (x:Narration {source_id: r.sid})
MERGE (e:Entity {entity_key: r.entity_key})
SET e += r.node
MERGE (x)-[:MENTIONS_ENTITY]->(e)
"""
ST_TOPIC = """
UNWIND $rows AS r
MATCH (x:Narration {source_id: r.sid})
MERGE (t:Topic {value: r.value})
MERGE (x)-[:HAS_TOPIC]->(t)
"""
ST_VERSE = """
UNWIND $rows AS r
MATCH (x:Narration {source_id: r.sid})
MERGE (v:QuranVerse {verse_key: r.verse_key})
SET v.surah = r.surah, v.ayah = r.ayah
MERGE (x)-[rr:REFERENCES {ref_type: r.ref_type, span: r.span}]->(v)
"""
ST_CANDIDATE = """
UNWIND $rows AS r
MATCH (x:Narration {source_id: r.sid})
MERGE (t:Topic {value: r.value})
MERGE (x)-[:CANDIDATE_TOPIC]->(t)
"""

#: deterministic RELATED_TO second pass (NO LLM): top-N other narrations
#: sharing >= min_shared exact topics, deterministic tie-break by source_id.
ST_RELATED = """
MATCH (a:Narration {source_id: $sid})-[:HAS_TOPIC]->(t:Topic)<-[:HAS_TOPIC]-(b:Narration)
WHERE b.source_id <> $sid
WITH b, count(DISTINCT t.value) AS shared
WHERE shared >= $min_shared
MATCH (a2:Narration {source_id: $sid})
WITH a2, b, shared
ORDER BY shared DESC, b.source_id ASC
LIMIT $top
MERGE (a2)-[r:RELATED_TO]->(b)
SET r.via = 'topics', r.score = shared
"""

#: (statement, row kind) in execution order — narration rows must land first
#: so the relationship statements can MATCH the source node.
_STATEMENT_ORDER: tuple[tuple[str, str], ...] = (
    (ST_NARRATION, "narration"),
    (ST_NARRATOR, "narrator"),
    (ST_ENTITY, "entity"),
    (ST_TOPIC, "topic"),
    (ST_VERSE, "verse"),
    (ST_CANDIDATE, "candidate"),
)

GRAPH_LABELS: tuple[str, ...] = (
    "Narration", "Book", "Chapter", "Narrator", "QuranVerse", "Topic", "Entity",
)
GRAPH_RELS: tuple[str, ...] = (
    "IN_BOOK", "IN_CHAPTER", "HAS_CHAPTER", "NARRATED_BY", "MENTIONS_ENTITY",
    "HAS_TOPIC", "CANDIDATE_TOPIC", "REFERENCES", "RELATED_TO",
)

_Q_EXPAND = """
UNWIND $sids AS sid
MATCH (n:Narration {{source_id: sid}})-[:{rel}]->(m)
RETURN n.source_id AS sid, collect(DISTINCT m.{prop}) AS items
"""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _chunks(seq, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _read_lines(path: Path) -> list[dict]:
    out = []
    p = Path(path)
    if not p.is_file():
        return out
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


def _dedupe_by_sid(lines: list[dict]) -> list[dict]:
    """Keep the last occurrence per source_id, preserve first-seen order."""
    by_id: dict[str, dict] = {}
    for l in lines:
        sid = l.get("source_id")
        if sid:
            by_id[sid] = l
    return list(by_id.values())


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --- keys / normalization --------------------------------------------------------

def normalize_topic(t: str) -> str:
    """Topic join key: lowercase, drop punctuation, collapse whitespace.

    Keeps latin + Arabic ranges (LLM topics are free text; normalization is
    the ONLY join key, by design).
    """
    t = (t or "").lower()
    t = re.sub(r"[^0-9a-z\u0600-\u06ff]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def book_key(book: str, volume: str) -> str:
    return f"{book or ''}|{volume or ''}"


def chapter_key(book: str, volume: str, part: str,
                chapter: str, section: str) -> str:
    return "|".join([book or "", volume or "", part or "",
                     chapter or "", section or ""])


def entity_key(type_: str, value: str) -> str:
    return f"{(type_ or 'concept').strip()}:{normalize_topic(value)}"


def verse_key(surah: int, ayah: int) -> str:
    return f"{surah}:{ayah}"


# --- ES index -------------------------------------------------------------------

def vector_mapping() -> dict:
    """semantic_vector mapping — copied from scripts/search/v2_mapping.json."""
    return {
        "type": "dense_vector",
        "dims": 1024,
        "index": True,
        "similarity": "cosine",
        "index_options": {"type": "int8_hnsw", "m": 16, "ef_construction": 100},
    }


def es_index_body() -> dict:
    """Full index body: exporter mapping + semantic_vector."""
    body = json.loads(json.dumps(exporter._es_mapping(), ensure_ascii=False))
    body["mappings"]["properties"]["semantic_vector"] = vector_mapping()
    return body


def _bulk_lines(docs: list[dict], index: str) -> list[str]:
    out = []
    for d in docs:
        sid = d.get("source_id") or ""
        out.append(json.dumps(
            {"index": {"_index": index, "_id": sid}}, ensure_ascii=False))
        out.append(json.dumps(d, ensure_ascii=False))
    return out


class EsClient:
    """Minimal ES REST client (stdlib-requests; mirrors LlmClient style)."""

    def __init__(self, base_url: str | None = None,
                 session: requests.Session | None = None,
                 timeout: float | None = None):
        self.base_url = (base_url or config.ES_URL).rstrip("/")
        self.timeout = timeout or ES_REQUEST_TIMEOUT_S
        self._session = session or requests.Session()

    def request(self, method: str, path: str, body=None):
        url = self.base_url + path
        try:
            r = self._session.request(method, url, json=body,
                                      timeout=self.timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"ES {method} {path} failed: {e}") from e
        if r.status_code >= 300:
            raise RuntimeError(
                f"ES {method} {path} -> HTTP {r.status_code}: {r.text[:300]}")
        return r.json()

    def index_exists(self, index: str) -> bool:
        r = self._session.get(self.base_url + "/" + index,
                              timeout=self.timeout)
        return r.status_code == 200

    def ensure_index(self, index: str) -> str:
        if self.index_exists(index):
            return "exists"
        self.request("PUT", "/" + index, es_index_body())
        return "created"

    def bulk(self, index: str, docs: list[dict]) -> dict:
        body = "\n".join(_bulk_lines(docs, index)) + "\n"
        try:
            r = self._session.post(
                self.base_url + "/_bulk", data=body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=ES_BULK_TIMEOUT_S)
        except requests.RequestException as e:
            raise RuntimeError(f"ES _bulk failed: {e}") from e
        if r.status_code >= 300:
            raise RuntimeError(f"ES _bulk -> HTTP {r.status_code}: "
                               f"{r.text[:300]}")
        data = r.json()
        errors = 0
        for item in data.get("items", []):
            it = item.get("index") or {}
            if it.get("result") not in ("created", "updated"):
                errors += 1
        return {"total": len(docs), "errors": errors}

    def search(self, index: str, body: dict) -> dict:
        return self.request("POST", f"/{index}/_search", body)

    def count(self, index: str) -> int:
        return int(self.request("GET", f"/{index}/_count").get("count", 0))


def es_stats(es: EsClient, index: str | None = None) -> dict:
    index = index or config.ES_INDEX
    info = es.request("GET", "/" + index)
    shards = (info.get("indices", {}).get(index, {})).get("primaries")
    return {"index": index, "count": es.count(index), "shards": shards,
            "health": info.get("status")}


def load_es(raw_path, snap_id: str, *, es: EsClient | None = None,
            index: str | None = None, enrich_path=None, embed_path=None,
            limit: int | None = None) -> dict:
    """Index export docs (+ enrichment + vectors) into ES. Idempotent.

    Doc id = source_id (bulk op ``index``) so a re-run only updates
    changed docs. Returns a summary dict.
    """
    index = index or config.ES_INDEX
    es = es or EsClient()
    raw_path = Path(raw_path)
    if not raw_path.is_file():
        raise FileNotFoundError(f"raw snapshot file not found: {raw_path}")
    edir = config.OUT_DIR / "enrich" / snap_id
    enr = enrich.load_enrichment(
        Path(enrich_path) if enrich_path else edir / enrich.OUTPUT_NAME)
    vectors = _load_vectors(
        Path(embed_path) if embed_path else edir / enrich.EMBED_NAME)

    lines = _dedupe_by_sid(_read_lines(raw_path))
    if limit is not None:
        lines = lines[:limit]

    es.ensure_index(index)

    docs: list[dict] = []
    no_vector = 0
    for line in lines:
        sid = line.get("source_id") or ""
        doc = exporter.build_export_doc(line, snap_id)
        enrich.apply_enrichment(doc, enr.get(sid))
        vec = vectors.get(sid)
        if vec is not None:
            doc["semantic_vector"] = vec
        else:
            no_vector += 1
        docs.append(doc)

    indexed = 0
    errors = 0
    for batch in _chunks(docs, BULK_BATCH):
        res = es.bulk(index, batch)
        indexed += res["total"] - res["errors"]
        errors += res["errors"]

    return {"index": index, "total": len(lines), "indexed": indexed,
            "errors": errors, "no_vector": no_vector, "enriched": len(enr),
            "vectors": len(vectors), "generated_at": _now()}


def _load_vectors(path: Path) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for rec in _read_lines(path):
        sid = rec.get("source_id")
        vec = rec.get("vector")
        if sid and isinstance(vec, list) and vec:
            out[sid] = vec
    return out


# --- Neo4j schema + loader --------------------------------------------------------

def open_neo4j(uri: str | None = None, user: str | None = None,
               password: str | None = None):
    """Lazily import the driver and connect."""
    try:
        import neo4j
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "neo4j driver not installed (pip install 'neo4j>=5.26,<6')") from e
    return neo4j.GraphDatabase.driver(
        uri or config.NEO4J_URI,
        auth=(user or config.NEO4J_USER,
              password if password is not None else config.NEO4J_PASSWORD))


def ensure_schema(driver) -> None:
    with driver.session() as s:
        for c in CONSTRAINTS:
            s.run(c)


def clear_graph(driver) -> None:
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def iter_graph_rows(line: dict, enr: dict | None, snap_id: str) -> dict:
    """Build all node/rel rows for one raw snapshot line.

    Returns a dict keyed by row kind: narration(1), narrator, entity,
    topic, verse, candidate. Row dicts carry both the node properties
    (``r.node``) and the join keys the MERGE statements need.
    """
    raw = line.get("raw") or {}
    sid = line.get("source_id") or raw.get("_id") or ""
    book = line.get("book") or raw.get("book") or ""
    fetched_at = line.get("fetched_at", "")
    vol = str(raw.get("volume") or "")
    part = str(raw.get("part") or "")
    chapter = str(raw.get("chapter") or "")
    section = str(raw.get("section") or "")
    bkey = book_key(book, vol)
    ckey = chapter_key(book, vol, part, chapter, section)

    persian = ""
    validation_status = ""
    if enr:
        p = enr.get("persian")
        if isinstance(p, str) and p.strip():
            persian = p
        val = (enr.get("provenance") or {}).get("validation") or {}
        validation_status = str(val.get("status") or "")

    rec_hash = normalize.normalize_doc(
        raw, book_slug=str(book).lower(), fetched_at=fetched_at)["record_hash"]

    rows: dict[str, list[dict]] = {
        "narration": [{
            "sid": sid,
            "book_key": bkey,
            "chapter_key": ckey,
            "node": {
                "hadith_number": str(raw.get("number") or ""),
                "book": book, "volume": vol, "part": part,
                "chapter": chapter, "section": section,
                "arabic": raw.get("arabic") or "",
                "english": raw.get("english") or "",
                "persian": persian,
                "record_hash": rec_hash,
                "snapshot_id": snap_id,
                "enriched": 1 if enr else 0,
                "validation_status": validation_status,
            },
            "book": {"book": book, "volume": vol,
                     "source": str(raw.get("source") or "")},
            "chapter": {"part": part, "part_ar": str(raw.get("part_ar") or ""),
                        "chapter": chapter, "section": section},
        }],
        "narrator": [], "entity": [], "topic": [], "verse": [],
        "candidate": [],
    }

    # narrators: chain order by s-offset (stable sort)
    en_by_p = {c.get("p"): c for c in raw.get("chain_links_en") or []
               if isinstance(c, dict)}
    chain = sorted(
        (c for c in raw.get("chain_links") or [] if isinstance(c, dict)
         and c.get("p")),
        key=lambda c: (c.get("s") or 0))
    for pos, c in enumerate(chain, 1):
        p = c["p"]
        rows["narrator"].append({
            "sid": sid, "narrator_id": p, "position": pos,
            "node": {"name_ar": str(c.get("t") or ""),
                     "name_en": str((en_by_p.get(p) or {}).get("t") or ""),
                     "slug": str(c.get("g") or "")},
        })

    if not enr:
        return rows

    for e in enr.get("entities") or []:
        v = str(e.get("value") or "").strip()
        if not v:
            continue
        etype = str(e.get("type") or "concept").strip()
        rows["entity"].append({
            "sid": sid, "entity_key": entity_key(etype, v),
            "node": {"type": etype, "value": v,
                     "note": str(e.get("note") or "").strip()},
        })

    seen_topics: set[str] = set()
    for t in enr.get("topics") or []:
        n = normalize_topic(t)
        if n and n not in seen_topics:
            seen_topics.add(n)
            rows["topic"].append({"sid": sid, "value": n})

    for q in enr.get("quran_references") or []:
        s, a = q.get("surah"), q.get("ayah")
        if not isinstance(s, int) or not isinstance(a, int):
            continue
        if s < 1 or s > 114 or a < 1:
            continue
        rows["verse"].append({
            "sid": sid, "verse_key": verse_key(s, a), "surah": s, "ayah": a,
            "ref_type": str(q.get("ref_type") or "mention"),
            "span": str(q.get("span") or "").strip(),
        })

    seen_cand: set[str] = set()
    for hc in enr.get("hadith_candidates") or []:
        n = normalize_topic(hc.get("topic"))
        if n and n not in seen_cand:
            seen_cand.add(n)
            rows["candidate"].append({"sid": sid, "value": n})

    return rows


def _exec_batch(driver, rows: dict[str, list[dict]]) -> None:
    with driver.session() as s:
        for st, kind in _STATEMENT_ORDER:
            part = rows.get(kind) or []
            if part:
                s.run(st, rows=part)


def _load_graph_state(path: Path, snap_id: str) -> dict | None:
    p = Path(path)
    if not p.is_file():
        return None
    try:
        state = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if state.get("snap_id") != snap_id:
        return None
    return state


def _save_graph_state(path: Path, snap_id: str, completed: set[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    state = {"snap_id": snap_id, "completed": sorted(completed),
             "updated_at": _now()}
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp.replace(p)


def load_graph(driver, raw_path, enr_map: dict[str, dict], *,
               snap_id: str | None = None, batch: int = GRAPH_BATCH,
               limit: int | None = None, out_dir: Path | None = None,
               rebuild: bool = False) -> dict:
    """Idempotent, checkpointed graph load (UNWIND-MERGE batches).

    Resume skips source_ids already in ``graph_state.json``; a rebuild
    (``rebuild=True``) does ``MATCH (n) DETACH DELETE n`` first.
    """
    raw_path = Path(raw_path)
    if not raw_path.is_file():
        raise FileNotFoundError(f"raw snapshot file not found: {raw_path}")
    snap_id = snap_id or raw_path.stem
    out_dir = Path(out_dir) if out_dir else config.OUT_DIR / "graph" / snap_id
    state_path = out_dir / GRAPH_STATE_NAME

    if rebuild:
        clear_graph(driver)
        completed: set[str] = set()
    else:
        ensure_schema(driver)
        state = _load_graph_state(state_path, snap_id)
        completed = set(state.get("completed", [])) if state else set()

    lines = _dedupe_by_sid(_read_lines(raw_path))
    if limit is not None:
        lines = lines[:limit]
    pending = [l for l in lines if l.get("source_id") not in completed]

    loaded = 0
    for chunk in _chunks(pending, batch):
        rows: dict[str, list[dict]] = {k: [] for _, k in _STATEMENT_ORDER}
        for line in chunk:
            sid = line.get("source_id") or ""
            r = iter_graph_rows(line, enr_map.get(sid), snap_id)
            for k in rows:
                rows[k].extend(r.get(k) or [])
        _exec_batch(driver, rows)
        for line in chunk:
            completed.add(line.get("source_id") or "")
            loaded += 1
        _save_graph_state(state_path, snap_id, completed)

    return {"snap_id": snap_id, "loaded": loaded,
            "skipped_completed": len(lines) - len(pending),
            "total": len(lines), "state": str(state_path),
            "generated_at": _now()}


def run_related_pass(driver, sids: list[str] | None = None,
                     top: int = RELATED_TOP,
                     min_shared: int = RELATED_MIN_SHARED) -> dict:
    """Deterministic RELATED_TO second pass (one statement per narration)."""
    if sids is None:
        with driver.session() as s:
            sids = [r["sid"] for r in s.run(
                "MATCH (n:Narration) RETURN n.source_id AS sid "
                "ORDER BY sid").data()]
    created = 0
    with driver.session() as s:
        for part in _chunks(list(sids), RELATED_SIDS_PER_QUERY):
            for sid in part:
                res = s.run(ST_RELATED, sid=sid, top=top,
                            min_shared=min_shared)
                created += res.consume().counters.relationships_created
    return {"narrations": len(sids), "related_created": created,
            "top": top, "min_shared": min_shared, "generated_at": _now()}


def expand_graph(driver, sids: list[str]) -> dict[str, dict]:
    """1-hop context pull for the given narrations (RAG expansion)."""
    out = {sid: {"narrators": [], "topics": [], "verses": [], "related": []}
           for sid in sids}
    if not sids:
        return out
    with driver.session() as s:
        for key, rel, prop in (("narrators", "NARRATED_BY", "narrator_id"),
                               ("topics", "HAS_TOPIC", "value"),
                               ("verses", "REFERENCES", "verse_key"),
                               ("related", "RELATED_TO", "source_id")):
            q = _Q_EXPAND.format(rel=rel, prop=prop)
            for row in s.run(q, sids=list(sids)).data():
                out[row["sid"]][key] = [x for x in row["items"]
                                        if x is not None]
    return out


def graph_stats(driver) -> dict:
    out = {"labels": {}, "relationships": {}}
    with driver.session() as s:
        for lbl in GRAPH_LABELS:
            out["labels"][lbl] = s.run(
                f"MATCH (n:{lbl}) RETURN count(n) AS c").single()["c"]
        for rel in GRAPH_RELS:
            out["relationships"][rel] = s.run(
                f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()["c"]
    return out


# --- RAG query path ---------------------------------------------------------------

def detect_lang(text: str) -> str:
    """Rough script detection: fa / ar / en (v1 heuristic).

    Persian-specific letters (پ چ ژ گ ک ی) mark Farsi; any other
    Arabic-range script is treated as Arabic; otherwise English.
    """
    t = text or ""
    for ch in t:
        o = ord(ch)
        if (0x0600 <= o <= 0x06FF or 0xFB50 <= o <= 0xFDFF
                or 0xFE70 <= o <= 0xFEFF):
            return "fa" if any(c in "پچژگکی" for c in t) else "ar"
    return "en"


def build_query_body(query: str, *, filters: dict | None = None,
                     top_k: int = 20) -> dict:
    """Trilingual multi_match over all text fields (+ structured filters)."""
    body = {
        "size": top_k,
        "query": {"multi_match": {
            "query": query,
            "fields": ["arabic", "english", "persian", "chapter", "book",
                       "narrator_en", "chapter_ar", "bab"],
            "type": "best_fields",
        }},
    }
    must: list[dict] = [body["query"]]
    f = filters or {}
    if f.get("book"):
        must.append({"term": {"book": f["book"]}})
    if f.get("hadith"):
        must.append({"term": {"hadith_number": str(f["hadith"])}})
    if f.get("volume"):
        must.append({"term": {"volume": str(f["volume"])}})
    if len(must) > 1:
        body["query"] = {"bool": {"must": must}}
    return body


def knn_body(query_vector: list[float], *, filters: dict | None = None,
             top_k: int = 20) -> dict:
    body = {
        "size": top_k,
        "knn": {"field": "semantic_vector", "query_vector": query_vector,
                "k": top_k, "num_candidates": max(top_k * 4, 50)},
    }
    f = filters or {}
    must: list[dict] = []
    if f.get("book"):
        must.append({"term": {"book": f["book"]}})
    if f.get("hadith"):
        must.append({"term": {"hadith_number": str(f["hadith"])}})
    if f.get("volume"):
        must.append({"term": {"volume": str(f["volume"])}})
    if must:
        body["query"] = {"bool": {"filter": must}}
    return body


def rrf_fuse(ranked: list[list[str]], k: int = RRF_K) -> list[tuple[str, float]]:
    """Reciprocal-rank fusion over ranked source_id lists (best first).

    Returns ``[(source_id, score), ...]`` sorted by score desc, then id
    (deterministic).
    """
    scores: dict[str, float] = {}
    for rl in ranked:
        for rank, sid in enumerate(rl):
            if not sid:
                continue
            scores[sid] = scores.get(sid, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))


def _mget_docs(es: EsClient, index: str, sids: list[str]) -> list[dict]:
    if not sids:
        return []
    res = es.request("POST", f"/{index}/_mget", {"ids": sids})
    by_id: dict[str, dict] = {}
    for d in res.get("docs", []):
        if d.get("found") and d.get("_source"):
            by_id[d["_id"]] = d["_source"]
    out = []
    for sid in sids:
        doc = by_id.get(sid)
        if doc is not None:
            doc.setdefault("source_id", sid)
            out.append(doc)
    return out


# --- citations ---------------------------------------------------------------------

_CITE_RE = re.compile(r"\u27e6(.*?)\u27e7")  # ⟦…⟧


def citation_text(doc: dict) -> str:
    """Canonical citation WITHOUT brackets: ``book · vol. V · part P · chapter · hadith N``."""
    parts = [doc.get("book") or ""]
    vol = doc.get("volume") or ""
    if vol:
        parts.append(f"vol. {vol}")
    part = doc.get("part") or ""
    if part:
        parts.append(f"part {part}")
    chap = doc.get("chapter") or doc.get("section") or ""
    if chap:
        parts.append(chap)
    num = doc.get("hadith_number") or ""
    if num:
        parts.append(f"hadith {num}")
    return " · ".join(parts)


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def verify_citations(answer: str, docs: list[dict]) -> dict:
    """Check every ⟦…⟧ in the answer against the context set."""
    allowed = {_norm_ws(citation_text(d)) for d in docs}
    found = _CITE_RE.findall(answer or "")
    verified = [f"\u27e6{c}\u27e7" for c in found
                if _norm_ws(c) in allowed]
    unverified = [f"\u27e6{c}\u27e7" for c in found
                  if _norm_ws(c) not in allowed]
    return {"verified": verified, "unverified": unverified,
            "verified_count": len(verified), "total": len(found)}


def drop_unverified_citations(answer: str, docs: list[dict]) -> str:
    allowed = {_norm_ws(citation_text(d)) for d in docs}
    return _CITE_RE.sub(
        lambda m: m.group(0) if _norm_ws(m.group(1)) in allowed else "",
        answer or "")


# --- RAG prompt + orchestration ------------------------------------------------------

RAG_SYSTEM = (
    "You are a research assistant answering questions about a hadith corpus "
    "(Arabic originals, English translations, Persian translations). "
    "Answer ONLY from the provided CONTEXT documents. "
    "Support every claim by copying, verbatim, the ⟦…⟧ citation marker of "
    "the context entry it comes from (the line beginning with the citation "
    "and a source id). If the context does not contain the answer, say so "
    "explicitly. Write the answer in the same language as the question."
)


def build_rag_prompt(query: str, lang: str,
                     context: list[dict]) -> list[dict]:
    blocks = []
    for i, d in enumerate(context, 1):
        blocks.append(
            f"[{i}] \u27e6{citation_text(d)}\u27e7 ({d.get('source_id', '')})\n"
            f"AR: {(d.get('arabic') or '')[:1500]}\n"
            f"EN: {(d.get('english') or '')[:600]}\n"
            f"FA: {(d.get('persian') or '')[:600]}\n")
    user = (f"QUESTION ({lang}):\n{query}\n\n"
            f"CONTEXT:\n" + "\n".join(blocks))
    return [{"role": "system", "content": RAG_SYSTEM},
            {"role": "user", "content": user}]


def _context_entry(d: dict, rank: int, origin: str) -> dict:
    return {"rank": rank, "origin": origin, "source_id": d.get("source_id"),
            "book": d.get("book"), "volume": d.get("volume"),
            "part": d.get("part"), "chapter": d.get("chapter"),
            "section": d.get("section"),
            "hadith_number": d.get("hadith_number"),
            "citation": f"\u27e6{citation_text(d)}\u27e7"}


def run_rag(query: str, *, lang: str = "auto", book: str | None = None,
            hadith=None, volume=None, top: int = 20, use_graph: bool = True,
            es: EsClient | None = None, driver=None, client=None,
            index: str | None = None,
            out_dir: Path | None = None) -> dict:
    """Full RAG path: hybrid ES retrieval -> RRF -> graph expansion ->
    LLM answer -> citation verification -> provenance log."""
    started = _now()
    lang = detect_lang(query) if lang in (None, "auto") else lang
    index = index or config.ES_INDEX
    es = es or EsClient()
    filters = {k: v for k, v in (("book", book), ("hadith", hadith),
                                 ("volume", volume)) if v}

    hits = es.search(index, build_query_body(query, filters=filters,
                                             top_k=top))
    text_ids = [h.get("_id") for h in hits.get("hits", {}).get("hits", [])]
    knn_ids: list[str] = []
    if client is not None:
        vec = client.embed([query], model=config.LLM_EMBED_MODEL)[0]
        kh = es.search(index, knn_body(vec, filters=filters, top_k=top))
        knn_ids = [h.get("_id") for h in kh.get("hits", {}).get("hits", [])]

    fused = rrf_fuse([text_ids, knn_ids], k=RRF_K)[:top]
    sids = [sid for sid, _ in fused]
    docs = _mget_docs(es, index, sids)

    graph_info: dict[str, dict] = {}
    extra_docs: list[dict] = []
    if use_graph and driver is not None and sids:
        graph_info = expand_graph(driver, sids[:top])
        seen = set(sids)
        related: list[str] = []
        for sid in sids[:top]:
            for m in graph_info.get(sid, {}).get("related", []):
                if m not in seen:
                    seen.add(m)
                    related.append(m)
        room = max(0, 2 * top - len(docs))
        extra_docs = _mget_docs(es, index, related[:room])

    context = docs + extra_docs
    result: dict = {
        "query": query, "lang": lang, "index": index, "started_at": started,
        "retrieval": {"text_hits": len(text_ids), "knn_hits": len(knn_ids),
                      "fused": sids,
                      "graph_expansion": graph_info or None,
                      "extra_docs": [d.get("source_id") for d in extra_docs]},
        "context": [_context_entry(d, i + 1,
                                   "retrieval" if i < len(docs)
                                   else "graph")
                    for i, d in enumerate(context)],
        "answer": None, "citations": None, "provenance": None,
    }

    if client is not None:
        messages = build_rag_prompt(query, lang, context)
        prompt_hash = _sha256_text(
            json.dumps(messages, ensure_ascii=False, sort_keys=True))
        res = client.chat(messages)
        answer = res.get("content") or ""
        cit = verify_citations(answer, context)
        if cit["unverified"]:
            answer = drop_unverified_citations(answer, context)
            cit["status"] = "partial_citation"
        else:
            cit["status"] = ("verified" if cit["total"] else "none")
        result["answer"] = answer
        result["citations"] = cit
        result["provenance"] = {
            "model": res.get("model"),
            "prompt_version": RAG_PROMPT_VERSION,
            "prompt_hash": prompt_hash,
            "latency_ms": res.get("latency_ms"),
            "completion_tokens": res.get("completion_tokens"),
            "finish_reason": res.get("finish_reason"),
            "budget": res.get("budget"),
        }

    out_dir = Path(out_dir) if out_dir else config.OUT_DIR / "rag"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result["generated_at"] = _now()
    log_path = out_dir / f"rag_{ts}.json"
    log_path.write_text(json.dumps(result, ensure_ascii=False, indent=1),
                        encoding="utf-8")
    result["provenance_log"] = str(log_path)
    return result
