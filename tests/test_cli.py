# -*- coding: utf-8 -*-
"""Tests for corpus_sync.cli (T10).

Subcommand behavior is pinned by file assertions: the CLI writes UTF-8
artifacts under DATA_DIR/out/ and exits 0 on success / 1 on failure.

- ``snapshot`` runs against a dict-driven fetch fake (monkeypatched on
  ``corpus_sync.fetch``) — no live network.
- ``detect`` / ``analyze`` / ``publish`` / ``validate`` run against the e2e
  fixture pair (snap_A/snap_B) installed into a tmp store.
- The sync.lock guard is exercised with fresh (<24h) and stale locks.
"""
import json
import os
import re
import time
from pathlib import Path

import huggingface_hub

from corpus_sync import cli, config, fetch as fetch_mod, snapshot, store
from corpus_sync.fetch import Book

FIX = Path(__file__).parent / "fixtures" / "e2e_snapshots"
BOOK = "Fa\u1e0da\u02beil al-Sh\u012b\u02bfa"   # Faḍāʾil al-Shīʿa (exact catalog name)
SNAP_A = "20261001-000000"
SNAP_B = "20261002-000000"


def _read_raw(path):
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _seed(tmp_data_dir):
    """Install the e2e pair into a tmp store: raw copy + records + detect."""
    import shutil
    from corpus_sync import detect

    raw = tmp_data_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    conn = store.open_db()
    for snap_id, fname, created in (
        (SNAP_A, "snap_A.jsonl", "2026-10-01T00:00:00Z"),
        (SNAP_B, "snap_B.jsonl", "2026-10-02T00:00:00Z"),
    ):
        dst = raw / fname
        shutil.copyfile(FIX / fname, dst)
        lines = _read_raw(dst)
        counts = {}
        for l in lines:
            counts[l["book"]] = counts.get(l["book"], 0) + 1
        store.record_snapshot(
            conn, snap_id=snap_id, created_at=created, status="complete",
            total_docs=len(lines), raw_path=str(dst), book_counts=counts)
        cli._store_records(conn, snap_id, dst)
    detect.detect_changes(conn, old_snap_id=SNAP_A, new_snap_id=SNAP_B)
    return conn


def _mini_docs(n=5, book="One"):
    return [
        {"_id": f"{book}:{i}", "book": book, "number": str(i),
         "chapter": f"Ch {i}", "arabic": f"arabic doc {i}.",
         "english": f"English doc {i}."}
        for i in range(1, n + 1)
    ]


class _Fetch:
    """Minimal books()/lookup() fake for the snapshot subcommand."""

    def __init__(self, docs):
        self.docs = docs
        self.calls = 0

    def books(self):
        return [Book(name="One", name_ar="One", slug="one", count=len(self.docs))]

    def lookup(self, q, page, per_page=config.PER_PAGE):
        self.calls += 1
        start = (page - 1) * per_page
        return snapshot.LookupResult(
            total=len(self.docs), collection=self.docs[start:start + per_page])


def _install_fetch(monkeypatch, docs):
    fake = _Fetch(docs)
    monkeypatch.setattr(fetch_mod, "books", fake.books)
    monkeypatch.setattr(fetch_mod, "lookup", fake.lookup)
    return fake


def _fake_hfapi(monkeypatch, calls):
    def _init(self, *a, **k):
        calls["init"] = k

    def _upload(self, **k):
        calls["upload"] = k

    cls = type("FakeHfApi", (), {"__init__": _init, "upload_folder": _upload})
    monkeypatch.setattr(huggingface_hub, "HfApi", cls)


# --- snapshot ---------------------------------------------------------------


def test_snapshot_subcommand(tmp_data_dir, monkeypatch):
    fake = _install_fetch(monkeypatch, _mini_docs(5))
    rc = cli.main(["snapshot"])
    assert rc == 0
    assert fake.calls == 1
    raw_files = list((tmp_data_dir / "raw").glob("*.jsonl"))
    assert len(raw_files) == 1
    summaries = list(config.OUT_DIR.glob("snapshot-*.summary.json"))
    assert len(summaries) == 1
    s = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert s["docs"] == 5
    assert s["books"] == {"One": 5}
    assert s["incomplete"] == []
    assert s["status"] == "complete"
    assert re.fullmatch(r"[0-9a-f]{64}", s["sha256"])
    assert isinstance(s["duration_s"], (int, float))
    # DB rows
    conn = store.open_db()
    assert store.latest_snapshot(conn) == s["snapshot"]
    assert store.snapshot_status(conn, s["snapshot"]) == "complete"
    assert len(store.load_record_hashes(conn, s["snapshot"])) == 5
    # lock released after a successful run
    assert not (config.STATE_DIR / cli.LOCK_NAME).exists()


def test_snapshot_refuses_fresh_lock(tmp_data_dir, monkeypatch):
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    (config.STATE_DIR / cli.LOCK_NAME).write_text("now", encoding="utf-8")

    def boom():
        raise AssertionError("fetch must not run while the lock is fresh")

    monkeypatch.setattr(fetch_mod, "books", boom)
    monkeypatch.setattr(fetch_mod, "lookup", boom)
    rc = cli.main(["snapshot"])
    assert rc == 1
    assert list((tmp_data_dir / "raw").glob("*.jsonl")) == []


def test_snapshot_allows_stale_lock(tmp_data_dir, monkeypatch):
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock = config.STATE_DIR / cli.LOCK_NAME
    lock.write_text("old", encoding="utf-8")
    old = time.time() - 25 * 3600
    os.utime(lock, (old, old))
    _install_fetch(monkeypatch, _mini_docs(3))
    rc = cli.main(["snapshot"])
    assert rc == 0


# --- detect -----------------------------------------------------------------


def test_detect_subcommand(tmp_data_dir):
    _seed(tmp_data_dir)
    rc = cli.main(["detect"])
    assert rc == 0
    out = config.OUT_DIR / f"changes-{SNAP_B}.json"
    assert out.is_file()
    d = json.loads(out.read_text(encoding="utf-8"))
    assert d["new_snapshot"] == SNAP_B
    assert d["old_snapshot"] == SNAP_A
    assert d["new"] == []
    assert d["modified"] == ["Fadail-al-Shia-Saduq:2"]
    assert d["removed"] == ["Fadail-al-Shia-Saduq:45"]
    assert d["unchanged"] == 43
    assert d["details"]["Fadail-al-Shia-Saduq:2"]["changed_fields"] == ["english"]


def test_detect_without_snapshots_fails(tmp_data_dir):
    assert cli.main(["detect"]) == 1


# --- analyze ----------------------------------------------------------------


def test_analyze_subcommand(tmp_data_dir):
    _seed(tmp_data_dir)
    rc = cli.main(["analyze", "--month", "2026-10"])
    assert rc == 0
    md = config.OUT_DIR / "report-2026-10.md"
    js = config.OUT_DIR / "report-2026-10.json"
    assert md.is_file() and js.is_file()
    m = json.loads(js.read_text(encoding="utf-8"))
    assert m["month"] == "2026-10"
    assert m["docs_total"] == 44
    assert m["docs_new"] == 0
    assert m["docs_modified"] == 1
    assert m["docs_removed"] == 1
    assert m["docs_unchanged"] == 43
    assert m["sentences_modified_en"] == 1
    assert m["sentences_modified_ar"] == 0
    assert m["sentences_removed_en"] >= 1
    assert m["vs_previous_month"]["old_snapshot"] == SNAP_A
    assert m["vs_previous_month"]["vs_previous_month"] is None
    assert m["vs_baseline"]["baseline_snapshot"] == SNAP_A
    assert m["vs_baseline"]["docs_removed"] == 1
    assert len(m["history"]) == 2
    text = md.read_text(encoding="utf-8")
    assert text.splitlines()[0] == "# Rewayaat corpus change report \u2014 2026-10"
    assert "44" in text
    assert "vs baseline" in text.lower()


# --- publish ----------------------------------------------------------------


def test_publish_subcommand(tmp_data_dir, monkeypatch):
    _seed(tmp_data_dir)
    calls = {}
    _fake_hfapi(monkeypatch, calls)
    monkeypatch.setenv("HF_TOKEN", "hf_test_token")
    rc = cli.main(["publish", "--month", "2026-10"])
    assert rc == 0
    staged = config.OUT_DIR / "publish"
    man_path = staged / "manifests" / "manifest.json"
    assert man_path.is_file()
    man = json.loads(man_path.read_text(encoding="utf-8"))
    assert man["per_book"] == {BOOK: 44}
    assert man["license"] == config.HF_LICENSE
    assert calls["init"]["token"] == "hf_test_token"
    assert calls["upload"]["repo_id"] == config.HF_REPO_ID
    assert calls["upload"]["private"] is True
    assert calls["upload"]["repo_type"] == "dataset"


# --- validate ---------------------------------------------------------------


def test_validate_subcommand(tmp_data_dir, monkeypatch):
    _seed(tmp_data_dir)
    monkeypatch.setattr(fetch_mod, "books",
                        lambda: [Book(name=BOOK, name_ar="x", slug="fadail", count=44)])
    assert cli.main(["validate"]) == 0
    monkeypatch.setattr(fetch_mod, "books",
                        lambda: [Book(name=BOOK, name_ar="x", slug="fadail", count=45)])
    assert cli.main(["validate"]) == 1
