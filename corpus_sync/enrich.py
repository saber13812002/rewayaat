# -*- coding: utf-8 -*-
"""Phase 3: LLM enrichment of the raw snapshot (trilingual + references).

Design (mission Phase 3):
- The raw snapshot is NEVER mutated. Output is a separate, keyed-by-
  ``source_id`` file (``enrichment.jsonl``) plus an optional
  ``embeddings.jsonl`` (``{source_id, vector}``). The exporter merges the
  enrichment into ``records.jsonl`` under the SAME ``source_id`` (filling the
  ``persian`` placeholder + an ``enrichment`` object).
- LLM output is NEVER authoritative. Every record carries a ``provenance``
  block: model, prompt_version, prompt_hash, latency_ms, completion_tokens,
  finish_reason, validation status + issues.
- Resume: ``enrich_state.json`` tracks completed/failed source_ids (same
  convention as ``snapshot_state.json``); a re-run skips completed ids and
  retries failed ones.

Gateway: OpenAI-compatible litellm (``http://127.0.0.1:4101`` on s52).
``gpt-oss-120b`` is a *reasoning* model (vLLM): it spends the completion
budget on ``reasoning_content`` first. Observed live: ``max_tokens=40`` ->
``content=null, finish_reason=length``; ``max_tokens=600`` -> content at
136 tokens / 0.7 s. Hence: always request a generous budget, and a null
content with ``finish_reason=length`` is a *budget* failure -> retry with an
escalated budget (see ``LlmClient.chat``).
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from . import config
from .snapshot import _dedupe

PROMPT_VERSION = config.ENRICH_PROMPT_VERSION

STATE_NAME = "enrich_state.json"
OUTPUT_NAME = "enrichment.jsonl"
EMBED_NAME = "embeddings.jsonl"
PROGRESS_NAME = "enrich_progress.log"
SUMMARY_NAME = "summary.json"
STATE_FLUSH_EVERY = 25
EMBED_BATCH = 16
MAX_BUDGET = 16384


class LlmError(RuntimeError):
    """Gateway call failed after all retries (or a non-retryable error)."""


class _BudgetExhausted(LlmError):
    """Reasoning model ate the completion budget (null content, finish=length)."""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- prompt -------------------------------------------------------------------

_PROMPT_TEMPLATE = """You are enriching a hadith (Islamic narration) from the book "{book}" (chapter: "{chapter}").

ARABIC ORIGINAL (authoritative text):
{arabic}

ENGLISH TRANSLATION (published; for reference only, NOT a translation source):
{english}

Produce a JSON object with EXACTLY these keys:
{{
  "persian": "<precise, fluent Persian (Farsi) translation of the Arabic original>",
  "entities": [{{"type": "person|place|concept|object", "value": "<name as it appears in the text>", "note": "<one short clause>"}}],
  "topics": ["<3-8 topical tags, lowercase English>"],
  "quran_references": [{{"surah": <int 1-114>, "ayah": <int >=1>, "ref_type": "quote|mention|allusion", "span": "<exact quoted or paraphrased text, empty if none>"}}],
  "hadith_candidates": [{{"topic": "<what related narration to search for>", "reason": "<why it may be related>"}}]
}}

RULES:
- "persian" must be a COMPLETE translation produced from the Arabic original (never a copy or edit of the English).
- "quran_references": include only verses the narration quotes, explicitly mentions, or clearly alludes to; surah/ayah must be exact. If unsure, omit the reference.
- "hadith_candidates": up to 3 search suggestions for related narrations (they may live in other books). Do NOT invent hadith numbers or source ids.
- "entities": only names, places, and concepts actually present in the text.
- Return ONLY the JSON object. No markdown fences, no commentary."""


def build_prompt(book: str, chapter: str, arabic: str, english: str) -> str:
    """Render the enrichment prompt (versioned: PROMPT_VERSION)."""
    return _PROMPT_TEMPLATE.format(
        book=book or "?", chapter=chapter or "?",
        arabic=arabic or "(missing)", english=english or "(missing)")


# --- LLM output parsing / validation -------------------------------------------

def parse_llm_json(text: str) -> dict:
    """Extract the first balanced JSON object from LLM output.

    Tolerates markdown code fences and surrounding commentary (repo
    convention, see scripts/quranic-insights/highlight_tafsir_snippets.py).
    """
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t.lstrip("`")
        if "```" in t:
            t = t.rsplit("```", 1)[0]
    start = t.find("{")
    if start == -1:
        raise ValueError("no JSON object in LLM output")
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(t)):
        c = t[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                obj = json.loads(t[start:i + 1])
                if not isinstance(obj, dict):
                    raise ValueError("LLM output is not a JSON object")
                return obj
    raise ValueError("unbalanced JSON object in LLM output")


def _has_persian_script(text: str) -> bool:
    return any(("\u0600" <= c <= "\u06FF" or "\u0750" <= c <= "\u077F"
                or "\uFB50" <= c <= "\uFDFF" or "\uFE70" <= c <= "\uFEFF")
               for c in text)


def validate_enrichment(obj: dict, arabic: str) -> list[str]:
    """Structural + sanity checks. Returns a list of issue codes (empty = pass).

    The checks are deliberately conservative: they catch broken outputs, not
    translation quality (that stays a human-review concern — LLM output is
    never authoritative).
    """
    issues: list[str] = []
    persian = obj.get("persian")
    if not isinstance(persian, str) or not persian.strip():
        issues.append("persian_missing")
    else:
        if not _has_persian_script(persian):
            issues.append("persian_not_persian_script")
        alen = len((arabic or "").strip())
        if alen and (len(persian.strip()) < 0.2 * alen
                     or len(persian.strip()) > 5 * alen):
            issues.append("persian_length_implausible")

    for key in ("entities", "topics", "quran_references", "hadith_candidates"):
        v = obj.get(key, [])
        if not isinstance(v, list):
            issues.append(f"{key}_not_list")

    for ref in (obj.get("quran_references") or []):
        if not isinstance(ref, dict):
            issues.append("quran_ref_not_object")
            continue
        s, a = ref.get("surah"), ref.get("ayah")
        if not (isinstance(s, int) and 1 <= s <= 114
                and isinstance(a, int) and a >= 1):
            issues.append("quran_ref_invalid")
        if ref.get("ref_type") not in ("quote", "mention", "allusion"):
            issues.append("quran_ref_type_invalid")

    return issues


def _validation_status(issues: list[str]) -> str:
    if "persian_missing" in issues:
        return "fail"
    return "warn" if issues else "pass"


# --- gateway client -------------------------------------------------------------

class LlmClient:
    """OpenAI-compatible chat + embeddings client (stdlib-requests based).

    Retry policy mirrors corpus_sync.http: transport errors, 408, 429 and 5xx
    are retried with exponential backoff; other 4xx fail immediately. A
    reasoning-model budget exhaustion — either content=null OR a TRUNCATED
    non-null content with finish_reason=length — retries with a doubled
    completion budget (capped at MAX_BUDGET). Truncated content must not be
    accepted: it would fail JSON parsing downstream.
    """

    def __init__(self, base_url: str | None = None, api_key: str | None = None,
                 model: str | None = None, timeout: float | None = None,
                 max_retries: int | None = None, backoff_s: float | None = None,
                 session: requests.Session | None = None):
        self.base_url = (base_url or config.LLM_BASE_URL).rstrip("/")
        self.api_key = api_key if api_key is not None else config.LLM_API_KEY
        self.model = model or config.LLM_CHAT_MODEL
        self.timeout = timeout if timeout is not None else config.LLM_REQUEST_TIMEOUT_S
        self.max_retries = max_retries if max_retries is not None else config.LLM_MAX_RETRIES
        self.backoff_s = backoff_s if backoff_s is not None else config.LLM_MIN_BACKOFF_S
        # Extra payload kwargs (e.g. chat_template_kwargs to disable Qwen
        # thinking mode). Captured at construction so process env wins.
        self.extra_body = config._extra_body()
        self._session = session or requests.Session()

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    @staticmethod
    def _retryable_status(status: int) -> bool:
        return status in (408, 429) or status >= 500

    def chat(self, messages: list[dict], *, max_tokens: int | None = None,
             temperature: float | None = None) -> dict:
        """POST /v1/chat/completions. Returns a result dict:

        {content, finish_reason, completion_tokens, latency_ms, model, budget}
        Raises LlmError when the call cannot be made (or after all retries).
        """
        url = f"{self.base_url}/v1/chat/completions"
        budget = max_tokens or config.LLM_MAX_TOKENS
        temp = temperature if temperature is not None else config.LLM_TEMPERATURE
        attempt = 0
        while True:
            attempt += 1
            payload = {
                "model": self.model,
                "messages": messages,
                "max_tokens": budget,
                "temperature": temp,
            }
            if self.extra_body:
                payload.update(self.extra_body)
            t0 = time.monotonic()
            try:
                r = self._session.post(url, json=payload, headers=self._headers(),
                                       timeout=self.timeout)
                latency_ms = int((time.monotonic() - t0) * 1000)
                if r.status_code >= 400:
                    raise _HttpFail(r.status_code, r.content[:200])
                data = r.json()
                choice = (data.get("choices") or [{}])[0]
                msg = choice.get("message") or {}
                content = msg.get("content")
                finish = choice.get("finish_reason")
                if not content:
                    if finish == "length":
                        raise _BudgetExhausted(
                            f"null content, finish_reason=length "
                            f"(budget {budget})")
                    raise LlmError(
                        f"null content, finish_reason={finish!r}")
                if finish == "length":
                    # Content exists but was cut off mid-generation (reasoning
                    # budget exhausted): a truncated JSON object will fail to
                    # parse downstream, so treat it like a budget failure.
                    raise _BudgetExhausted(
                        f"truncated content, finish_reason=length "
                        f"(budget {budget}, {len(content)} chars)")
                usage = data.get("usage") or {}
                return {
                    "content": content,
                    "finish_reason": finish,
                    "completion_tokens": usage.get("completion_tokens"),
                    "latency_ms": latency_ms,
                    "model": self.model,
                    "budget": budget,
                }
            except requests.RequestException as e:
                # NOTE: assign (do NOT raise) so the sibling `except _HttpFail`
                # clause below still classifies it — a raise inside an except
                # handler escapes the whole try/except chain.
                exc = _HttpFail(None, str(e)[:200])
                retryable = True  # transport-level failure: always retryable
            except _HttpFail as hf:
                exc, retryable = hf, self._retryable_status(hf.status or 0)
            except _BudgetExhausted as be:
                exc, retryable = be, True
                budget = min(budget * 2, MAX_BUDGET)
            if not retryable or attempt >= self.max_retries:
                raise LlmError(f"POST {url} failed after {attempt} attempt(s): "
                               f"{exc}") from exc
            time.sleep(self.backoff_s * 2 ** (attempt - 1))

    def embed(self, texts: list[str], *, model: str | None = None) -> list[list[float]]:
        """POST /v1/embeddings; returns vectors in input order."""
        url = f"{self.base_url}/v1/embeddings"
        payload = {"model": model or config.LLM_EMBED_MODEL, "input": texts}
        attempt = 0
        while True:
            attempt += 1
            try:
                r = self._session.post(url, json=payload, headers=self._headers(),
                                       timeout=self.timeout)
                if r.status_code >= 400:
                    raise _HttpFail(r.status_code, r.content[:200])
                data = r.json()
                rows = sorted(data.get("data") or [], key=lambda d: d.get("index", 0))
                return [d["embedding"] for d in rows]
            except requests.RequestException as e:
                exc = _HttpFail(None, str(e)[:200])
            except _HttpFail as hf:
                exc = hf
            if not self._retryable_status(exc.status or 0) or attempt >= self.max_retries:
                raise LlmError(f"POST {url} failed after {attempt} attempt(s): "
                               f"{exc}") from exc
            time.sleep(self.backoff_s * 2 ** (attempt - 1))


class _HttpFail(Exception):
    def __init__(self, status: int | None, body: str):
        super().__init__(f"HTTP {status}: {body}" if status else body)
        self.status = status
        self.body = body


# --- per-doc enrichment -----------------------------------------------------------

def enrich_one(client: LlmClient, line: dict) -> dict:
    """Enrich one raw snapshot line. Returns the enrichment record.

    ``line`` is a snapshot JSONL line: {source_id, book, fetched_at, page,
    label, raw}. Raises LlmError / ValueError on gateway or output failure
    (the caller records it in state and moves on).
    """
    raw = line.get("raw") or {}
    book = line.get("book") or raw.get("book") or ""
    chapter = raw.get("chapter") or ""
    arabic = raw.get("arabic") or ""
    english = raw.get("english") or ""

    prompt = build_prompt(book, chapter, arabic, english)
    result = client.chat([{"role": "user", "content": prompt}])
    obj = parse_llm_json(result["content"])
    issues = validate_enrichment(obj, arabic)

    persian = obj.get("persian")
    return {
        "source_id": line.get("source_id") or raw.get("_id") or "",
        "persian": persian if isinstance(persian, str) else None,
        "entities": obj.get("entities") or [],
        "topics": obj.get("topics") or [],
        "quran_references": obj.get("quran_references") or [],
        "hadith_candidates": obj.get("hadith_candidates") or [],
        "provenance": {
            "model": result["model"],
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "latency_ms": result["latency_ms"],
            "completion_tokens": result["completion_tokens"],
            "finish_reason": result["finish_reason"],
            "validation": {"status": _validation_status(issues),
                           "issues": issues},
            "generated_at": _now(),
        },
    }


# --- checkpointed run ---------------------------------------------------------------

def _read_lines(path: Path) -> list[dict]:
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _line_sid(line: dict) -> str:
    return line.get("source_id") or (line.get("raw") or {}).get("_id") or ""


def _load_state(path: Path, snap_id: str) -> dict | None:
    if not path.is_file():
        return None
    try:
        s = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(s, dict) or s.get("snap_id") != snap_id:
        return None
    s.setdefault("completed", [])
    s.setdefault("failed", {})
    return s


def _save_state(path: Path, state: dict) -> None:
    state["last_activity"] = _now()
    # Merge state saved by other parallel workers (book-partitioned runs) so
    # concurrent checkpointers never drop each other's completed ids.
    try:
        other = _load_state(path, state.get("snap_id"))
    except Exception:  # noqa: BLE001 - best-effort merge
        other = None
    if other:
        seen = set(state["completed"])
        for sid in other["completed"]:
            if sid not in seen:
                state["completed"].append(sid)
        for sid, info in other["failed"].items():
            state["failed"].setdefault(sid, info)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1),
                   encoding="utf-8")
    os.replace(tmp, path)


def _progress_line(out_dir: Path, level: str, msg: str) -> None:
    line = f"[{_now()}] {level} {msg}"
    print(line)
    try:
        with open(out_dir / PROGRESS_NAME, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def run_enrichment(raw_path: Path, snap_id: str, *, client: LlmClient,
                   out_dir: Path | None = None, resume: bool = True,
                   limit: int | None = None,
                   books: list[str] | None = None) -> dict:
    """Enrich every doc of a raw snapshot, checkpointed by source_id.

    Writes ``enrichment.jsonl`` (append-only), ``enrich_state.json``
    (resume point) and ``summary.json`` under ``out_dir``
    (default ``DATA_DIR/out/enrich/<snap_id>``). Returns the summary dict.
    """
    out_dir = Path(out_dir) if out_dir else config.OUT_DIR / "enrich" / snap_id
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / STATE_NAME
    out_path = out_dir / OUTPUT_NAME

    lines = _dedupe(_read_lines(Path(raw_path)))
    total_docs = len(lines)
    if books:
        wanted = set(books)
        lines = [l for l in lines
                 if (l.get("book") or (l.get("raw") or {}).get("book")) in wanted]

    state = _load_state(state_path, snap_id) if resume else None
    if state is None:
        state = {"snap_id": snap_id, "completed": [], "failed": {}}
        out_path.write_text("", encoding="utf-8")  # fresh start truncates
    completed = set(state["completed"])
    pending = [l for l in lines if _line_sid(l) not in completed]
    if limit is not None:
        pending = pending[:limit]

    done = 0
    with open(out_path, "a", encoding="utf-8") as fh:
        for i, line in enumerate(pending, 1):
            sid = _line_sid(line)
            try:
                rec = enrich_one(client, line)
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
                state["completed"].append(sid)
                state["failed"].pop(sid, None)
                vstatus = rec["provenance"]["validation"]["status"]
                _progress_line(out_dir, "INFO" if vstatus == "pass" else "WARN",
                               f"[{i}/{len(pending)}] {sid} {vstatus}")
            except Exception as e:  # noqa: BLE001 - record and continue
                prev = state["failed"].get(sid, {}).get("attempts", 0)
                state["failed"][sid] = {
                    "error": f"{type(e).__name__}: {e}"[:300],
                    "attempts": prev + 1,
                    "last": _now(),
                }
                _progress_line(out_dir, "ERROR",
                               f"[{i}/{len(pending)}] {sid} {type(e).__name__}: "
                               f"{str(e)[:120]}")
            done += 1
            if done % STATE_FLUSH_EVERY == 0:
                _save_state(state_path, state)
    _save_state(state_path, state)

    summary = {
        "snap_id": snap_id,
        "status": "complete" if not state["failed"] else "partial",
        "total_docs": total_docs,
        "enriched": len(state["completed"]),
        "failed": len(state["failed"]),
        "failed_ids": sorted(state["failed"])[:50],
        "processed_this_run": done,
        "prompt_version": PROMPT_VERSION,
        "model": getattr(client, "model", "unknown"),
        "out_dir": str(out_dir),
        "generated_at": _now(),
    }
    (out_dir / SUMMARY_NAME).write_text(
        json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    return summary


# --- export merge + embeddings ------------------------------------------------------

def load_enrichment(path: Path) -> dict[str, dict]:
    """Load enrichment.jsonl into {source_id: record} (later lines win)."""
    p = Path(path)
    if not p.is_file():
        return {}
    return {sid: rec for rec in _read_lines(p)
            if (sid := rec.get("source_id"))}


def apply_enrichment(doc: dict, enr: dict | None) -> None:
    """Merge one enrichment record into an export doc (same source_id).

    Fills the ``persian`` placeholder and attaches the full record under
    ``enrichment``. Raw/export fields are otherwise untouched.
    """
    if not enr:
        return
    persian = enr.get("persian")
    if isinstance(persian, str) and persian.strip():
        doc["persian"] = persian
    doc["enrichment"] = enr


def build_embed_text(arabic: str, english: str, persian: str) -> str:
    """Trilingual embedding input. Phase 4 MUST reuse this exact format so
    training and inference stay aligned."""
    parts = []
    ar = (arabic or "").strip()
    if len(ar) > 3000:
        ar = ar[:3000].strip()
    if ar:
        parts.append(ar)
    en = (english or "").strip()[:300]
    if en:
        parts.append(en)
    fa = (persian or "").strip()[:500]
    if fa:
        parts.append(fa)
    return " || ".join(parts)


def run_embeddings(raw_path: Path, snap_id: str, *, client: LlmClient,
                   out_dir: Path | None = None,
                   embed_model: str | None = None) -> dict:
    """Vectorize enriched docs (arabic || english || persian) via the gateway
    embedding model. Appends to ``embeddings.jsonl`` under ``out_dir``."""
    out_dir = Path(out_dir) if out_dir else config.OUT_DIR / "enrich" / snap_id
    out_path = out_dir / EMBED_NAME
    model = embed_model or config.LLM_EMBED_MODEL

    enr = load_enrichment(out_dir / OUTPUT_NAME)
    raw_by_id: dict[str, dict] = {}
    for line in _read_lines(Path(raw_path)):
        sid = _line_sid(line)
        if sid:
            raw_by_id[sid] = line

    rows: list[tuple[str, str]] = []
    for sid, rec in enr.items():
        line = raw_by_id.get(sid) or {}
        raw = line.get("raw") or {}
        text = build_embed_text(raw.get("arabic") or "",
                                raw.get("english") or "",
                                rec.get("persian") or "")
        if text:
            rows.append((sid, text))

    written = 0
    with open(out_path, "a", encoding="utf-8") as fh:
        for i in range(0, len(rows), EMBED_BATCH):
            chunk = rows[i:i + EMBED_BATCH]
            vectors = client.embed([t for _, t in chunk], model=model)
            for (sid, _), vec in zip(chunk, vectors):
                fh.write(json.dumps(
                    {"source_id": sid, "model": model, "dim": len(vec),
                     "vector": vec}, ensure_ascii=False) + "\n")
                written += 1
            fh.flush()
            os.fsync(fh.fileno())
    return {"snap_id": snap_id, "model": model, "vectors": written,
            "out_dir": str(out_dir), "generated_at": _now()}
