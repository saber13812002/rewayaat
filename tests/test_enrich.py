# -*- coding: utf-8 -*-
"""Tests for Phase 3 LLM enrichment — corpus_sync.enrich.

No network: the gateway is faked with a canned-response ``requests.Session``
stand-in (see ``_FakeSession``) for the real ``LlmClient`` and a lightweight
``FakeClient`` (dict-returning ``chat``/``embed``) for the pipeline functions.

Covered:
- parse_llm_json (plain / fenced / trailing text / no object / unbalanced)
- validate_enrichment (pass / persian_missing->fail / not-Persian /
  length implausible / list-not-list / bad Quran ref / bad ref_type)
- build_prompt shape (no stray braces, book/chapter/texts present)
- enrich_one provenance block (model, prompt_version, prompt_hash, ...)
- LlmClient.chat retry classification (429/5xx retried, 4xx immediate,
  transport retried, exhaustion) + reasoning-model budget escalation
- LlmClient.embed (returns vectors in input order, retry)
- run_enrichment (3-doc run -> lines + state + summary; resume skips
  completed = idempotent; failed retried on resume; --limit; books filter;
  --no-resume truncates)
- load_enrichment (later line wins, missing file -> {})
- apply_enrichment (fills persian + attaches enrichment; None -> untouched)
- build_embed_text (arabic || english || persian + truncation)
- run_embeddings (batched, joins raw + enrichment)
- exporter merge (persian filled, enrichment nested, parquet column,
  manifest entry, ES mapping property)
"""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import requests

from corpus_sync import enrich, exporter


# --- canned texts -------------------------------------------------------------

AR_LONG = "قال رسول الله صلى الله عليه وسلم في هذا الباب كلاما طويلا مفصلا"
FA_VALID = "پیامبر گرامی خدا در این باب سخن بلند و مفصلی فرمودند"


def _valid_obj() -> dict:
    return {
        "persian": FA_VALID,
        "entities": [{"type": "person", "value": "امام", "note": "n"}],
        "topics": ["test", "topic"],
        "quran_references": [
            {"surah": 1, "ayah": 1, "ref_type": "mention", "span": ""}],
        "hadith_candidates": [{"topic": "t", "reason": "r"}],
    }


VALID_JSON = json.dumps(_valid_obj(), ensure_ascii=False)


# --- fakes --------------------------------------------------------------------

class _FakeResp:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body
        if isinstance(body, (bytes, bytearray)):
            self.content = bytes(body)
        elif isinstance(body, str):
            self.content = body.encode("utf-8")
        else:
            self.content = json.dumps(body, ensure_ascii=False).encode("utf-8")

    def json(self):
        return self._body


class _FakeSession:
    """Pops canned responses in call order; records (url, payload) calls."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append((url, json))
        if not self.responses:
            raise AssertionError("no more canned responses")
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        if isinstance(r, _FakeResp):
            return r
        status, body = r
        return _FakeResp(status, body)


def _chat_ok(content=VALID_JSON, finish="stop", completion_tokens=136):
    return (200, {"choices": [
        {"message": {"content": content}, "finish_reason": finish}],
        "usage": {"completion_tokens": completion_tokens}})


def _chat_null(finish="length"):
    return (200, {"choices": [
        {"message": {"content": None}, "finish_reason": finish}]})


class FakeClient:
    """Dict-returning stand-in for LlmClient (pipeline-level tests)."""

    def __init__(self, responder, model="gpt-oss-120b", embed_dim=3):
        self._responder = responder
        self.model = model
        self._embed_dim = embed_dim
        self.chat_calls = 0
        self.embed_calls = []

    def chat(self, messages, **kw):
        self.chat_calls += 1
        return self._responder(messages, self.chat_calls)

    def embed(self, texts, **kw):
        self.embed_calls.append(list(texts))
        return [[0.1 * (i + 1) for i in range(self._embed_dim)] for _ in texts]


def _ok_responder(*_, **__):
    return {"content": VALID_JSON, "finish_reason": "stop",
            "completion_tokens": 136, "latency_ms": 50,
            "model": "gpt-oss-120b", "budget": 2048}


# --- raw snapshot fixture -------------------------------------------------------

def _write_raw(path: Path, specs) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for sid, book, arabic, english in specs:
            try:
                number = int(sid[1:])
            except ValueError:
                number = 1
            raw = {"_id": sid, "book": book, "chapter": "Chapter One",
                   "arabic": arabic, "english": english, "number": number}
            line = {"source_id": sid, "book": book,
                    "fetched_at": "2026-01-01T00:00:00Z", "page": 1,
                    "label": f"{book}:1", "raw": raw}
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")


def _read(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines()
            if l.strip()]


# --- parse_llm_json -------------------------------------------------------------

def test_parse_plain():
    assert enrich.parse_llm_json(VALID_JSON)["persian"] == FA_VALID


def test_parse_fenced():
    text = "```json\n" + VALID_JSON + "\n```"
    assert enrich.parse_llm_json(text)["persian"] == FA_VALID


def test_parse_trailing_text():
    assert enrich.parse_llm_json(VALID_JSON + " done talking")["persian"] == FA_VALID


def test_parse_leading_comment():
    assert enrich.parse_llm_json("Here you go:\n" + VALID_JSON)["persian"] == FA_VALID


def test_parse_no_object():
    import pytest
    with pytest.raises(ValueError):
        enrich.parse_llm_json("no braces here at all")


def test_parse_unbalanced():
    import pytest
    with pytest.raises(ValueError):
        enrich.parse_llm_json('{"persian": "سلام"')  # missing close brace


def test_parse_braces_in_string():
    obj = enrich.parse_llm_json('{"persian": "x { y } z", "topics": []}')
    assert obj["persian"] == "x { y } z"


# --- validate_enrichment --------------------------------------------------------

def _v(obj) -> tuple[str, list]:
    issues = enrich.validate_enrichment(obj, AR_LONG)
    return enrich._validation_status(issues), issues


def test_validate_pass():
    status, issues = _v(_valid_obj())
    assert status == "pass" and issues == []


def test_validate_persian_missing_is_fail():
    obj = _valid_obj()
    obj["persian"] = ""
    status, issues = _v(obj)
    assert status == "fail" and "persian_missing" in issues


def test_validate_persian_not_persian_script():
    obj = _valid_obj()
    obj["persian"] = "this is a plain english sentence, long enough here"
    status, issues = _v(obj)
    assert status == "warn" and "persian_not_persian_script" in issues


def test_validate_length_implausible():
    obj = _valid_obj()
    obj["persian"] = "سلام"  # far too short for AR_LONG
    status, issues = _v(obj)
    assert "persian_length_implausible" in issues


def test_validate_list_not_list():
    obj = _valid_obj()
    obj["topics"] = "not-a-list"
    _, issues = _v(obj)
    assert "topics_not_list" in issues


def test_validate_bad_quran_ref():
    obj = _valid_obj()
    obj["quran_references"] = [{"surah": 200, "ayah": 0, "ref_type": "quote"}]
    _, issues = _v(obj)
    assert "quran_ref_invalid" in issues


def test_validate_bad_ref_type():
    obj = _valid_obj()
    obj["quran_references"] = [
        {"surah": 1, "ayah": 1, "ref_type": "weird"}]
    _, issues = _v(obj)
    assert "quran_ref_type_invalid" in issues


# --- build_prompt ---------------------------------------------------------------

def test_build_prompt_shape():
    p = enrich.build_prompt("Al-Amālī", "Ch One", AR_LONG, "English text")
    assert "Al-Amālī" in p and "Ch One" in p
    assert AR_LONG in p and "English text" in p
    assert "persian" in p and "quran_references" in p
    # no stray unformatted braces left by .format()
    assert "{" in p  # template keeps the JSON skeleton braces


def test_build_prompt_missing_fields():
    p = enrich.build_prompt("", "", "", "")
    assert "?" in p and "(missing)" in p


# --- enrich_one provenance ------------------------------------------------------

def test_enrich_one_provenance():
    raw = {"_id": "s1", "book": "B", "chapter": "C",
           "arabic": AR_LONG, "english": "en"}
    line = {"source_id": "s1", "book": "B", "raw": raw}
    rec = enrich.enrich_one(FakeClient(_ok_responder), line)
    assert rec["source_id"] == "s1"
    assert rec["persian"] == FA_VALID
    prov = rec["provenance"]
    assert prov["model"] == "gpt-oss-120b"
    assert prov["prompt_version"] == enrich.PROMPT_VERSION
    assert len(prov["prompt_hash"]) == 64
    assert prov["validation"]["status"] == "pass"
    assert prov["validation"]["issues"] == []
    assert prov["finish_reason"] == "stop"
    assert "generated_at" in prov


def test_enrich_one_records_fail_when_persian_missing():
    def responder(messages, n):
        return {"content": json.dumps({"persian": None, "topics": []}),
                "finish_reason": "stop", "completion_tokens": 10,
                "latency_ms": 5, "model": "m", "budget": 2048}
    line = {"source_id": "s9", "book": "B",
            "raw": {"_id": "s9", "book": "B", "chapter": "C",
                    "arabic": AR_LONG, "english": "en"}}
    rec = enrich.enrich_one(FakeClient(responder), line)
    assert rec["provenance"]["validation"]["status"] == "fail"
    assert rec["persian"] is None


# --- LlmClient.chat retry + budget ---------------------------------------------

def test_chat_budget_escalation(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([
        _chat_null("length"),  # first: budget exhausted -> escalate
        _chat_ok(VALID_JSON),  # second: success
    ])
    client = enrich.LlmClient(base_url="http://x", api_key="k",
                              model="m", timeout=1, max_retries=3,
                              backoff_s=0, session=sess)
    res = client.chat([{"role": "user", "content": "p"}])
    assert res["content"] == VALID_JSON
    budgets = [c[1]["max_tokens"] for c in sess.calls]
    assert budgets[0] == 2048
    assert budgets[1] == 4096, "budget must double after null-content length"


def test_chat_429_retried_then_ok(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([(429, "rate limited"), _chat_ok()])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    client.chat([{"role": "user", "content": "p"}])
    assert len(sess.calls) == 2


def test_chat_5xx_retried_then_ok(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([(503, "unavailable"), _chat_ok()])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    client.chat([{"role": "user", "content": "p"}])
    assert len(sess.calls) == 2


def test_chat_4xx_fails_immediately(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([(404, "not found")])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    import pytest
    with pytest.raises(enrich.LlmError):
        client.chat([{"role": "user", "content": "p"}])
    assert len(sess.calls) == 1, "4xx must not be retried"


def test_chat_transport_retried_then_ok(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([requests.exceptions.ConnectionError("conn"),
                         _chat_ok()])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    client.chat([{"role": "user", "content": "p"}])
    assert len(sess.calls) == 2


def test_chat_exhausts_retries(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([(500, "e"), (500, "e"), (500, "e")])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    import pytest
    with pytest.raises(enrich.LlmError):
        client.chat([{"role": "user", "content": "p"}])
    assert len(sess.calls) == 3


def test_chat_null_stop_is_non_retryable(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([_chat_null("stop")])  # null content, not length
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    import pytest
    with pytest.raises(enrich.LlmError):
        client.chat([{"role": "user", "content": "p"}])
    assert len(sess.calls) == 1, "null content w/o length is not a budget fail"


# --- LlmClient.embed -------------------------------------------------------------

def test_embed_returns_input_order(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    body = {"data": [
        {"index": 1, "embedding": [1.0, 2.0]},
        {"index": 0, "embedding": [3.0, 4.0]},
    ]}
    sess = _FakeSession([(200, body)])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    vecs = client.embed(["a", "b"], model="emb")
    assert vecs == [[3.0, 4.0], [1.0, 2.0]]  # sorted by index, not arrival
    assert sess.calls[0][1]["model"] == "emb"


def test_embed_retries_5xx(monkeypatch):
    monkeypatch.setattr(enrich.time, "sleep", lambda s: None)
    sess = _FakeSession([(500, "e"), (200, {"data": [
        {"index": 0, "embedding": [9.0]}]})])
    client = enrich.LlmClient(base_url="http://x", api_key="k", model="m",
                              timeout=1, max_retries=3, backoff_s=0,
                              session=sess)
    vecs = client.embed(["x"])
    assert vecs == [[9.0]]
    assert len(sess.calls) == 2


# --- run_enrichment ---------------------------------------------------------------

def test_run_enrichment_three_docs(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "en1"),
                     ("s2", "BookA", AR_LONG, "en2"),
                     ("s3", "BookA", AR_LONG, "en3")])
    out = tmp_path / "enrich"
    client = FakeClient(_ok_responder)
    summary = enrich.run_enrichment(raw, "snap1", client=client, out_dir=out)

    assert summary["status"] == "complete"
    assert summary["enriched"] == 3 and summary["failed"] == 0
    assert client.chat_calls == 3
    recs = _read(out / "enrichment.jsonl")
    assert [r["source_id"] for r in recs] == ["s1", "s2", "s3"]
    state = json.loads((out / "enrich_state.json").read_text(encoding="utf-8"))
    assert state["completed"] == ["s1", "s2", "s3"]
    assert state["failed"] == {}
    assert (out / "summary.json").is_file()
    assert (out / "enrich_progress.log").is_file()


def test_run_enrichment_resume_is_idempotent(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "e"),
                     ("s2", "BookA", AR_LONG, "e"),
                     ("s3", "BookA", AR_LONG, "e")])
    out = tmp_path / "enrich"
    enrich.run_enrichment(raw, "snap1", client=FakeClient(_ok_responder),
                          out_dir=out)
    # second run: everything completed -> zero new chat calls
    client2 = FakeClient(_ok_responder)
    summary = enrich.run_enrichment(raw, "snap1", client=client2, out_dir=out)
    assert summary["processed_this_run"] == 0
    assert client2.chat_calls == 0
    assert summary["enriched"] == 3
    # enrichment.jsonl NOT re-appended (still 3 lines)
    assert len(_read(out / "enrichment.jsonl")) == 3


def test_run_enrichment_failed_retried_on_resume(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "e"),
                     ("s2", "BookA", AR_LONG, "e"),
                     ("s3", "BookA", AR_LONG, "e")])
    out = tmp_path / "enrich"

    def fail_second(messages, n):
        if n == 2:  # s2 fails on the first run
            raise enrich.LlmError("boom")
        return _ok_responder(messages, n)

    enrich.run_enrichment(raw, "snap1", client=FakeClient(fail_second),
                          out_dir=out)
    state = json.loads((out / "enrich_state.json").read_text(encoding="utf-8"))
    assert set(state["completed"]) == {"s1", "s3"}
    assert "s2" in state["failed"]

    # resume with a healthy client: only s2 is pending
    client2 = FakeClient(_ok_responder)
    summary = enrich.run_enrichment(raw, "snap1", client=client2, out_dir=out)
    assert summary["status"] == "complete"
    assert summary["enriched"] == 3 and summary["failed"] == 0
    assert client2.chat_calls == 1
    sids = [r["source_id"] for r in _read(out / "enrichment.jsonl")]
    assert sids.count("s2") == 1 and len(sids) == 3


def test_run_enrichment_limit(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "e"),
                     ("s2", "BookA", AR_LONG, "e"),
                     ("s3", "BookA", AR_LONG, "e")])
    out = tmp_path / "enrich"
    summary = enrich.run_enrichment(raw, "snap1", client=FakeClient(_ok_responder),
                                    out_dir=out, limit=1)
    assert summary["enriched"] == 1
    assert len(_read(out / "enrichment.jsonl")) == 1
    # resume to finish the rest
    summary = enrich.run_enrichment(raw, "snap1", client=FakeClient(_ok_responder),
                                    out_dir=out)
    assert summary["enriched"] == 3
    assert len(_read(out / "enrichment.jsonl")) == 3


def test_run_enrichment_books_filter(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "e"),
                     ("s2", "BookA", AR_LONG, "e"),
                     ("s3", "BookB", AR_LONG, "e")])
    out = tmp_path / "enrich"
    client = FakeClient(_ok_responder)
    summary = enrich.run_enrichment(raw, "snap1", client=client, out_dir=out,
                                    books=["BookA"])
    assert summary["enriched"] == 2
    sids = [r["source_id"] for r in _read(out / "enrichment.jsonl")]
    assert sids == ["s1", "s2"]  # BookB (s3) untouched


def test_run_enrichment_no_resume_truncates(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "e"),
                     ("s2", "BookA", AR_LONG, "e")])
    out = tmp_path / "enrich"
    enrich.run_enrichment(raw, "snap1", client=FakeClient(_ok_responder),
                          out_dir=out)
    # fresh start: truncate + reprocess
    summary = enrich.run_enrichment(raw, "snap1", client=FakeClient(_ok_responder),
                                    out_dir=out, resume=False)
    assert summary["processed_this_run"] == 2
    assert len(_read(out / "enrichment.jsonl")) == 2  # not 4


# --- load / apply / embed-text ---------------------------------------------------

def test_load_enrichment_later_wins(tmp_path):
    p = tmp_path / "enrichment.jsonl"
    p.write_text(
        json.dumps({"source_id": "a", "persian": "old"}) + "\n"
        + json.dumps({"source_id": "a", "persian": "new"}) + "\n",
        encoding="utf-8")
    d = enrich.load_enrichment(p)
    assert d["a"]["persian"] == "new"


def test_load_enrichment_missing(tmp_path):
    assert enrich.load_enrichment(tmp_path / "nope.jsonl") == {}


def test_apply_enrichment_fills():
    doc = {"source_id": "a", "persian": None, "enrichment": None}
    enr = {"source_id": "a", "persian": FA_VALID, "topics": ["t"]}
    enrich.apply_enrichment(doc, enr)
    assert doc["persian"] == FA_VALID
    assert doc["enrichment"] == enr


def test_apply_enrichment_none_untouched():
    doc = {"source_id": "a", "persian": None, "enrichment": None}
    enrich.apply_enrichment(doc, None)
    assert doc["persian"] is None and doc["enrichment"] is None


def test_apply_enrichment_empty_persian_kept_none():
    doc = {"persian": None, "enrichment": None}
    enrich.apply_enrichment(doc, {"persian": "  ", "topics": []})
    assert doc["persian"] is None
    assert doc["enrichment"] is not None


def test_build_embed_text_format():
    t = enrich.build_embed_text(AR_LONG, "English here", FA_VALID)
    assert t == f"{AR_LONG} || English here || {FA_VALID}"


def test_build_embed_text_truncation_and_empty():
    ar = "خ" * 4000
    en = "e" * 400
    fa = "ف" * 600
    t = enrich.build_embed_text(ar, en, fa)
    parts = t.split(" || ")
    assert len(parts[0]) <= 3000
    assert len(parts[1]) <= 300
    assert len(parts[2]) <= 500
    assert enrich.build_embed_text("", "", "") == ""
    assert enrich.build_embed_text(AR_LONG, "", "") == AR_LONG


# --- run_embeddings ---------------------------------------------------------------

def test_run_embeddings_batches_and_joins(tmp_data_dir, tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "en1"),
                     ("s2", "BookA", AR_LONG, "en2")])
    out = tmp_path / "enrich"
    enrich.run_enrichment(raw, "snap1", client=FakeClient(_ok_responder),
                          out_dir=out)
    client = FakeClient(_ok_responder, embed_dim=4)
    summary = enrich.run_embeddings(raw, "snap1", client=client, out_dir=out,
                                    embed_model="emb-model")
    assert summary["vectors"] == 2
    assert summary["model"] == "emb-model"
    recs = _read(out / "embeddings.jsonl")
    assert [r["source_id"] for r in recs] == ["s1", "s2"]
    assert recs[0]["dim"] == 4
    # embed was called with the trilingual text, batched
    assert client.embed_calls[0][0].startswith(AR_LONG + " || en1 || ")


# --- exporter merge -----------------------------------------------------------------

def _make_enrichment(path: Path, sids) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for sid in sids:
            fh.write(json.dumps({
                "source_id": sid, "persian": FA_VALID,
                "entities": [], "topics": ["t"],
                "quran_references": [], "hadith_candidates": [],
                "provenance": {"model": "m", "prompt_version": "1.0",
                               "prompt_hash": "h", "latency_ms": 1,
                               "completion_tokens": 1, "finish_reason": "stop",
                               "validation": {"status": "pass", "issues": []},
                               "generated_at": "2026-01-01T00:00:00Z"},
            }, ensure_ascii=False) + "\n")


def test_exporter_merges_enrichment(tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "en1"),
                     ("s2", "BookA", AR_LONG, "en2"),
                     ("s3", "BookA", AR_LONG, "en3")])
    enr = tmp_path / "enrichment.jsonl"
    _make_enrichment(enr, ["s1", "s2"])  # s3 not enriched

    dest = tmp_path / "export"
    manifest = exporter.build_export(dest, raw_path=raw, snap_id="snapE",
                                     status="complete", enrichment_path=enr)

    docs = _read(dest / "records.jsonl")
    by_id = {d["source_id"]: d for d in docs}
    assert by_id["s1"]["persian"] == FA_VALID
    assert by_id["s1"]["enrichment"]["provenance"]["model"] == "m"
    assert by_id["s2"]["persian"] == FA_VALID
    assert by_id["s3"]["persian"] is None
    assert by_id["s3"]["enrichment"] is None

    # parquet carries the enrichment column (canonical JSON / "" )
    rows = pq.read_table(dest / "records.parquet").to_pylist()
    enriched_rows = [r for r in rows if r["enrichment"]]
    assert len(enriched_rows) == 2
    for r in enriched_rows:
        assert json.loads(r["enrichment"])["persian"] == FA_VALID
    assert "enrichment" in rows[0]

    # manifest records the enrichment source + count
    assert manifest["enrichment"]["enriched_docs"] == 2
    assert manifest["enrichment"]["source"] == str(enr)


def test_exporter_no_enrichment_defaults_null(tmp_path):
    raw = tmp_path / "raw.jsonl"
    _write_raw(raw, [("s1", "BookA", AR_LONG, "en1")])
    dest = tmp_path / "export"
    manifest = exporter.build_export(dest, raw_path=raw, snap_id="snapN")
    assert manifest["enrichment"] is None
    d = _read(dest / "records.jsonl")[0]
    assert d["persian"] is None and d["enrichment"] is None
    assert list(d.keys()) == list(exporter._ALL_COLS)


def test_es_mapping_has_enrichment():
    props = exporter._es_mapping()["mappings"]["properties"]
    assert "enrichment" in props
    e = props["enrichment"]["properties"]
    assert e["persian"]["type"] == "text"
    assert e["topics"]["type"] == "keyword"
    q = e["quran_references"]["properties"]
    assert q["surah"]["type"] == "integer" and q["ayah"]["type"] == "integer"
    assert e["provenance"]["properties"]["validation"]["properties"]["status"] \
        ["type"] == "keyword"
