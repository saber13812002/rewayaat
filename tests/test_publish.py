"""Tests for corpus_sync.publish — HF dataset staging + private upload (T9).

Synthetic store: two complete snapshots (Jan 100 docs, Feb 101 docs with
2 new / 2 modified / 1 removed) + detect_changes, then:

- ``build_dataset_dir`` lays out the spec dataset tree; the manifest's
  sha256 values match the staged files (and cover every staged file but
  the manifest itself); the README is a dataset card carrying the
  private-until-license notice; the parquet is readable with the full
  record count.
- ``upload_dataset`` resolves the token (arg > HF_TOKEN env >
  PublishError), passes ``private=config.HF_IS_PRIVATE`` to
  ``HfApi().upload_folder``, and the token value never appears in any
  staged file.
"""
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from corpus_sync import config, detect, publish, store


def _raw_doc(num: int, book: str) -> dict:
    return {
        "_id": f"Book:{num}", "book": book, "number": str(num),
        "chapter": f"Chapter {num}",
        "arabic": f"جملة عربي رقم {num} .",
        "english": f"English sentence number {num}.",
    }


def _write_raw(path: Path, docs: list[dict], fetched: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for d in docs:
            fh.write(json.dumps(
                {"source_id": d["_id"], "book": d["book"],
                 "fetched_at": fetched, "page": 1, "label": "book",
                 "raw": d}, ensure_ascii=False) + "\n")


def _install(conn, snap_id: str, created: str, raw: Path, fetched: str,
             docs: list[dict]) -> None:
    _write_raw(raw, docs, fetched)
    store.record_snapshot(conn, snap_id=snap_id, created_at=created,
                          status="complete", total_docs=len(docs),
                          raw_path=str(raw),
                          book_counts={b: sum(1 for d in docs if d["book"] == b)
                                       for b in ("B1", "B2", "B3")})
    from corpus_sync import normalize
    store.insert_records(conn, snap_id,
                         [normalize.normalize_doc(d, book_slug=d["book"].lower(),
                                                   fetched_at=fetched)
                          for d in docs])


def _docs_100() -> list[dict]:
    docs = [_raw_doc(n, "B1") for n in range(1, 31)]
    docs += [_raw_doc(n, "B2") for n in range(31, 81)]
    docs += [_raw_doc(n, "B3") for n in range(81, 101)]
    return docs


def _docs_next(docs100: list[dict]) -> list[dict]:
    docs = [dict(d) for d in docs100]
    docs = [d for d in docs if d["_id"] != "Book:41"]
    for d in docs:
        if d["_id"] == "Book:22":
            d["arabic"] = d["arabic"][:-2] + " مضاف. زيادة."
        if d["_id"] == "Book:42":
            d["english"] = d["english"][:-1] + " Extra."
    docs.append({"_id": "Book:104", "book": "B1", "number": "104",
                 "chapter": "Chapter 104", "arabic": "جديد .",
                 "english": "New one."})
    docs.append({"_id": "Book:105", "book": "B3", "number": "105",
                 "chapter": "Chapter 105", "arabic": "جديد .",
                 "english": "New two."})
    return docs


SNAP_A = "20260101-000000"
SNAP_B = "20260201-000000"


def _fixture_store(tmp_path: Path):
    conn = store.open_db(tmp_path / "db.sqlite")
    d100 = _docs_100()
    _install(conn, SNAP_A, "2026-01-01T00:00:00Z", tmp_path / "m1.jsonl",
             "2026-01-01T00:00:00Z", d100)
    d101 = _docs_next(d100)
    _install(conn, SNAP_B, "2026-02-01T00:00:00Z", tmp_path / "m2.jsonl",
             "2026-02-01T00:00:00Z", d101)
    detect.detect_changes(conn, old_snap_id=SNAP_A, new_snap_id=SNAP_B)
    return conn


def _staged(tmp_path: Path, conn) -> Path:
    return publish.build_dataset_dir(tmp_path / "hf", conn=conn,
                                     new_snap_id=SNAP_B, month="2026-02")


def test_build_dataset_dir_layout_and_manifest(tmp_path):
    conn = _fixture_store(tmp_path)
    dest = _staged(tmp_path, conn)

    for rel in ("data/current/normalized.parquet",
                "data/current/normalized.jsonl",
                f"data/raw/{SNAP_B}.jsonl",
                "changes/2026-02.jsonl",
                "reports/2026-02.md",
                "manifests/manifest.json",
                "README.md"):
        assert (dest / rel).is_file(), rel

    # full normalized record per doc
    norm = (dest / "data/current/normalized.jsonl").read_text("utf-8").splitlines()
    assert len(norm) == 101
    first = json.loads(norm[0])
    assert set(first) >= {"source_id", "book", "chapter", "hadith_number",
                          "arabic", "english", "metadata", "record_hash",
                          "arabic_hash", "english_hash", "metadata_hash",
                          "normalization_version", "raw"}

    # raw copy is byte-identical to the snapshot file
    assert (dest / f"data/raw/{SNAP_B}.jsonl").read_bytes() == \
        (tmp_path / "m2.jsonl").read_bytes()

    # changes: one line per NEW/MODIFIED/REMOVED doc (5 total)
    ch = [json.loads(l) for l in
          (dest / "changes/2026-02.jsonl").read_text("utf-8").splitlines()]
    assert len(ch) == 5
    assert {c["doc_status"] for c in ch} == {"NEW", "MODIFIED", "REMOVED"}

    # report rendered for the month
    report = (dest / "reports/2026-02.md").read_text("utf-8")
    assert "# Rewayaat corpus change report — 2026-02" in report

    # manifest
    man = json.loads((dest / "manifests/manifest.json").read_text("utf-8"))
    assert man["repo"] == config.HF_REPO_ID
    assert man["month"] == "2026-02"
    assert man["snapshot"] == SNAP_B
    assert man["created_at"]
    assert man["docs_total"] == 101
    assert man["per_book"] == {"B1": 31, "B2": 49, "B3": 21}
    assert man["norm_version"] == config.NORM_VERSION
    assert man["license"] == "UNCONFIRMED - private"
    # every manifest sha256 matches its file ...
    for rel, expected in man["sha256"].items():
        actual = hashlib.sha256((dest / rel).read_bytes()).hexdigest()
        assert actual == expected, rel
    # ... and every staged file but the manifest is covered
    covered = set(man["sha256"])
    for p in dest.rglob("*"):
        if p.is_file() and p.name != "manifest.json":
            assert p.relative_to(dest).as_posix() in covered

    # parquet readable with the full record count
    tbl = pq.read_table(dest / "data/current/normalized.parquet")
    assert tbl.num_rows == 101
    assert set(tbl.column_names) >= {"source_id", "arabic", "english",
                                     "record_hash", "normalization_version"}


def test_upload_dataset_private_and_token_hygiene(tmp_path, monkeypatch):
    conn = _fixture_store(tmp_path)
    dest = _staged(tmp_path, conn)
    calls: dict = {}

    class _FakeHfApi:
        def __init__(self, *a, **k):
            calls["init"] = k

        def upload_folder(self, **k):
            calls["upload"] = k

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)
    monkeypatch.delenv("HF_TOKEN", raising=False)

    url = publish.upload_dataset(dest, token="hf_sentinel_TOKEN_123")
    assert url == f"https://huggingface.co/datasets/{config.HF_REPO_ID}"
    up = calls["upload"]
    assert up["repo_id"] == config.HF_REPO_ID
    assert up["repo_type"] == "dataset"
    assert up["private"] is True          # config.HF_IS_PRIVATE
    assert up["create_repo"] is True
    assert up["folder_path"] == dest
    assert calls["init"]["token"] == "hf_sentinel_TOKEN_123"
    # the token value is in no staged file
    for p in dest.rglob("*"):
        if p.is_file():
            assert b"hf_sentinel_TOKEN_123" not in p.read_bytes(), p


def test_upload_dataset_env_token_and_missing(tmp_path, monkeypatch):
    conn = _fixture_store(tmp_path)
    dest = _staged(tmp_path, conn)
    calls: dict = {}

    class _FakeHfApi:
        def __init__(self, *a, **k):
            calls["init"] = k

        def upload_folder(self, **k):
            calls["upload"] = k

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    monkeypatch.setenv("HF_TOKEN", "hf_from_env")
    publish.upload_dataset(dest)
    assert calls["init"]["token"] == "hf_from_env"

    monkeypatch.delenv("HF_TOKEN")
    with pytest.raises(publish.PublishError) as ei:
        publish.upload_dataset(dest)
    assert "HF_TOKEN" in str(ei.value)
