# -*- coding: utf-8 -*-
"""Tests for the standalone corpus export (Phase 2) — corpus_sync.exporter.

Built on the real e2e fixture snapshot (tests/fixtures/e2e_snapshots/
snap_A.jsonl, 45 docs of Faḍaʾil al-Shīʿa) and the full 31-field API doc
fixture (tests/fixtures/hadith_alamali_1.json).

Covered:
- record count / column completeness / stable source_id keys
- manifest sha256 integrity (every shipped file) + manifest self-hash scheme
- parquet ⇄ jsonl equivalence (flat + nested-as-canonical-JSON)
- raw.jsonl is a byte-identical copy of the source snapshot
- hashes match the pinned normalizer baseline
- ES mapping covers every exported field; _id = source_id
- idempotent rebuild (identical artifact hashes)
- CLI `export` subcommand (store-backed + --allow-partial)
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from corpus_sync import cli, exporter, normalize, store

FIXTURES = Path(__file__).resolve().parent / "fixtures"
SNAP_A = FIXTURES / "e2e_snapshots" / "snap_A.jsonl"
HADITH_1 = FIXTURES / "hadith_alamali_1.json"


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _lines(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def test_build_export_core(tmp_path):
    dest = tmp_path / "export"
    manifest = exporter.build_export(dest, raw_path=SNAP_A,
                                     snap_id="snap-A-test", status="complete")

    assert manifest["docs_total"] == 45
    assert manifest["snapshot_id"] == "snap-A-test"
    assert manifest["snapshot_status"] == "complete"
    assert manifest["books"] == {"Faḍaʾil al-Shīʿa": 45}

    for name in ("records.jsonl", "records.parquet", "raw.jsonl",
                 "manifest.json", "data_dictionary.md", "mappings.json",
                 "README.md"):
        assert (dest / name).is_file(), name

    docs = _lines(dest / "records.jsonl")
    assert len(docs) == 45
    # every column present, in the declared order
    for d in docs:
        assert list(d.keys()) == list(exporter._ALL_COLS)
    ids = [d["source_id"] for d in docs]
    assert len(set(ids)) == 45, "source_id must be unique"

    # raw.jsonl is a byte-identical provenance copy
    assert _sha256(dest / "raw.jsonl") == _sha256(SNAP_A)

    # manifest: every listed file verifies
    m = json.loads((dest / "manifest.json").read_text(encoding="utf-8"))
    assert m["docs_total"] == 45
    for f in m["files"]:
        assert _sha256(dest / f["path"]) == f["sha256"], f["path"]
    assert {f["path"] for f in m["files"]} == {
        "records.jsonl", "records.parquet", "raw.jsonl",
        "data_dictionary.md", "mappings.json", "README.md"}


def test_manifest_self_hash(tmp_path):
    dest = tmp_path / "export"
    exporter.build_export(dest, raw_path=SNAP_A, snap_id="snap-A-test")
    mtext = (dest / "manifest.json").read_text(encoding="utf-8")
    m = json.loads(mtext)
    m2 = dict(m)
    m2["manifest_sha256"] = ""
    base = json.dumps(m2, ensure_ascii=False, indent=1).encode("utf-8")
    assert m["manifest_sha256"] == hashlib.sha256(base).hexdigest()


def test_parquet_jsonl_equivalence(tmp_path):
    dest = tmp_path / "export"
    exporter.build_export(dest, raw_path=SNAP_A, snap_id="snap-A-test")
    docs = _lines(dest / "records.jsonl")
    table = pq.read_table(dest / "records.parquet")
    rows = table.to_pylist()
    assert len(rows) == len(docs)
    for row, doc in zip(rows, docs):
        for c in exporter._FLAT_STRING_COLS:
            assert row[c] == doc[c], c
        for c in exporter._NESTED_COLS:
            expect = (normalize.canonical_json(doc[c])
                      if doc[c] is not None else "")
            assert row[c] == expect, c


def test_hash_baseline_and_fields(tmp_path):
    dest = tmp_path / "export"
    exporter.build_export(dest, raw_path=SNAP_A, snap_id="snap-A-test")
    docs = _lines(dest / "records.jsonl")
    raw_lines = _lines(SNAP_A)
    for line, doc in zip(raw_lines, docs):
        n = normalize.normalize_doc(line["raw"], book_slug=line["book"].lower(),
                                    fetched_at=line["fetched_at"])
        assert doc["record_hash"] == n["record_hash"]
        assert doc["arabic_hash"] == n["arabic_hash"]
        assert doc["english_hash"] == n["english_hash"]
        assert doc["metadata_hash"] == n["metadata_hash"]
        assert doc["arabic"] == n["arabic"]
        assert doc["english"] == n["english"]
        assert doc["bab"] == doc["chapter"]
        assert doc["persian"] is None
        assert doc["normalization_version"] == "1.0"
        assert doc["snapshot_id"] == "snap-A-test"
        assert doc["provenance"]["record_hash"] == n["record_hash"]
        # full raw doc carried verbatim
        assert doc["raw"] == line["raw"]


def test_build_export_doc_rich_fixture():
    line = {
        "source_id": "Al-Amali-Saduq:1",
        "book": "Al-Amālī",
        "fetched_at": "2026-01-01T00:00:00Z",
        "page": 1,
        "label": "Al-Amālī:1",
        "raw": json.loads(HADITH_1.read_text(encoding="utf-8")),
    }
    doc = exporter.build_export_doc(line, "snap-X")
    assert doc["source_id"] == "Al-Amali-Saduq:1"
    assert doc["book"] == "Al-Amālī"
    assert doc["book_slug"] == "al-amālī".lower()
    chain_people = line["raw"]["chain_people"]
    assert doc["narrator"] == normalize._narrator_from_chain(chain_people)
    # narrator_en = last chain_links_en entry's name
    links_en = line["raw"]["chain_links_en"]
    assert doc["narrator_en"] == links_en[-1]["t"]
    assert doc["hadith_number"] == str(line["raw"]["number"])
    assert doc["metadata"].get("number") == line["raw"]["number"]
    assert "_id" not in doc["metadata"]
    assert doc["raw"] == line["raw"]
    assert list(doc.keys()) == list(exporter._ALL_COLS)


def test_es_mapping_covers_all_fields():
    mapping = exporter._es_mapping()
    props = mapping["mappings"]["properties"]
    for c in exporter._ALL_COLS:
        assert c in props, f"missing mapping for {c}"
    # analyzers must match the production search index convention
    analysis = mapping["settings"]["index"]["analysis"]
    assert "arabic_norm" in analysis["analyzer"]
    assert "english_fold" in analysis["analyzer"]
    assert "strip_hamza" in analysis["char_filter"]
    assert "translit_names" in analysis["filter"]
    # and must be identical to scripts/search/v2_mapping.json
    v2 = json.loads((Path(__file__).resolve().parent.parent
                    / "scripts" / "search" / "v2_mapping.json"
                    ).read_text(encoding="utf-8"))
    ref = v2["settings"]["index"]["analysis"]
    for key in ("arabic_norm", "english_fold", "english_fold_search"):
        assert analysis["analyzer"][key] == ref["analyzer"][key], key
    assert analysis["char_filter"] == ref["char_filter"]
    assert analysis["filter"]["translit_names"] == ref["filter"]["translit_names"]


def test_es_doc_id_roundtrip(tmp_path):
    dest = tmp_path / "export"
    exporter.build_export(dest, raw_path=SNAP_A, snap_id="snap-A-test")
    docs = _lines(dest / "records.jsonl")
    # each line is a complete ES doc indexable as POST /{idx}/_doc/{source_id}
    for d in docs:
        assert d["source_id"]
        body = json.dumps(d, ensure_ascii=False).encode("utf-8")
        assert json.loads(body)["source_id"] == d["source_id"]
    # idempotency of the bulk op is by construction: same _id, no version bump
    # needed (verified operationally in Phase 5 against real ES).


def test_idempotent_rebuild(tmp_path):
    d1, d2 = tmp_path / "e1", tmp_path / "e2"
    for d in (d1, d2):
        exporter.build_export(d, raw_path=SNAP_A, snap_id="snap-A-test",
                              status="complete")
    for name in ("records.jsonl", "records.parquet", "raw.jsonl",
                 "data_dictionary.md", "mappings.json", "README.md"):
        assert _sha256(d1 / name) == _sha256(d2 / name), name
    # manifest differs only in generated_at (wall clock) — verify everything
    # except that field is identical
    m1 = json.loads((d1 / "manifest.json").read_text(encoding="utf-8"))
    m2 = json.loads((d2 / "manifest.json").read_text(encoding="utf-8"))
    m1.pop("generated_at"), m2.pop("generated_at")
    assert m1 == m2


def test_verify_live_books(tmp_path):
    class B:
        def __init__(self, name, count):
            self.name, self.count = name, count

    dest = tmp_path / "export"
    manifest = exporter.build_export(
        dest, raw_path=SNAP_A, snap_id="snap-A-test",
        verify_live_books=[B("Faḍaʾil al-Shīʿa", 45), B("Al-Kāfī", 100)])
    cc = manifest["completeness_check"]
    assert cc["live_total"] == 145
    assert cc["incomplete"] == ["Al-Kāfī"]


def test_cli_export(tmp_data_dir, monkeypatch, capsys):
    raw = tmp_data_dir / "raw" / "snap-A.jsonl"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(SNAP_A.read_bytes())
    conn = store.open_db()
    store.record_snapshot(conn, snap_id="20260101-000000-1",
                          created_at="2026-01-01T00:00:00Z",
                          status="complete", total_docs=45,
                          raw_path="raw/snap-A.jsonl",
                          book_counts={"Faḍaʾil al-Shīʿa": 45})
    assert cli.main(["export"]) == 0
    out = capsys.readouterr().out
    assert "export 20260101-000000-1" in out
    dest = tmp_data_dir / "out" / "export" / "20260101-000000-1"
    assert (dest / "records.jsonl").is_file()
    assert len(_lines(dest / "records.jsonl")) == 45

    # partial snapshot: refused by default, accepted with --allow-partial
    conn.execute("UPDATE snapshots SET status='partial' WHERE id=?",
                 ("20260101-000000-1",))
    conn.commit()
    assert cli.main(["export"]) == 1
    assert cli.main(["export", "--allow-partial",
                     "--dest", str(tmp_data_dir / "out" / "export" / "p")]) == 0
    m = json.loads((tmp_data_dir / "out" / "export" / "p" / "manifest.json")
                   .read_text(encoding="utf-8"))
    assert m["snapshot_status"] == "partial"

    # --raw override works standalone (no store row needed)
    assert cli.main(["export", "--raw", str(raw),
                     "--dest", str(tmp_data_dir / "out" / "export" / "r")]) == 0
    m = json.loads((tmp_data_dir / "out" / "export" / "r" / "manifest.json")
                   .read_text(encoding="utf-8"))
    assert m["snapshot_id"] == "snap-A"
    assert m["docs_total"] == 45

    # missing raw file -> exit 1
    assert cli.main(["export", "--raw", str(tmp_data_dir / "nope.jsonl")]) == 1
