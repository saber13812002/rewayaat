# -*- coding: utf-8 -*-
"""Corpus Status API — live, read-only status endpoint (contract v1.0).

Contract: docs/corpus-sync/corpus-status-api-v1.md (versioned; kept in sync
with both repos — rewayaat and persian-corpus-toolkit).

Design rules
------------
- READ-ONLY: never writes pipeline data. The only writes are its own
  history file (``data/status/history.jsonl``) and log output.
- REAL DATA ONLY: every number is read from a live source (SQLite,
  Elasticsearch, Neo4j, HTTP health, on-disk checkpoints, process table)
  at request time. If a source is unreachable the field is ``null`` and
  the stage ``status`` reflects that — values are never fabricated.
- STDLIB ONLY: http.server / sqlite3 / urllib / subprocess. Runnable with
  the plain repo venv (``.venv/bin/python -m corpus_sync.statusapi``).
- MULTI-CORPUS: one instance serves one corpus (``STATUS_CORPUS_ID``).
  Adding a new corpus = new instance + env file, no code change.

Endpoints
---------
GET /               -> service name, api_version, corpus_id, endpoint list
GET /health         -> liveness {status, corpus_id, uptime_s}
GET /v1/status      -> full status document (schema in contract doc)
GET /v1/summary     -> compact card (top-line numbers + overall health)
GET /v1/history     -> hourly history records (newest last)
GET /v1/errors      -> recent errors from checkpoints + logs

Deployment (s52): systemd user unit ``statusapi.service``, port 8190.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

API_VERSION = "1.0"
POLL_S = int(os.environ.get("STATUS_POLL_S", "60"))
WINDOW_MAX = 24 * 60           # 60 s samples => up to 24 h in-memory
HTTP_TIMEOUT_S = float(os.environ.get("STATUS_HTTP_TIMEOUT_S", "5"))
RECENT_ERROR_LINES = 5

STARTED_AT = time.time()

# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _http(url: str, headers: dict | None = None,
          timeout: float = HTTP_TIMEOUT_S) -> tuple[int, Any, float]:
    """GET *url*; returns (status_code, parsed_json_or_text, latency_ms)."""
    req = urllib.request.Request(url, headers=headers or {})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            ms = (time.monotonic() - t0) * 1000
            try:
                return r.status, json.loads(raw.decode("utf-8", "replace")), ms
            except ValueError:
                return r.status, raw[:300].decode("utf-8", "replace"), ms
    except urllib.error.HTTPError as e:
        return e.code, f"HTTPError {e.code}", (time.monotonic() - t0) * 1000
    except Exception as e:  # noqa: BLE001 - network/timeout -> report down
        return 0, f"{type(e).__name__}: {e}"[:200], (time.monotonic() - t0) * 1000


def _pgrep(pattern: str) -> list[str]:
    """Return 'pid cmdline' lines matching *pattern* (empty on non-Linux)."""
    if not shutil.which("pgrep"):
        return []
    try:
        r = subprocess.run(["pgrep", "-af", pattern], capture_output=True,
                           text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return []
    self_pid = str(os.getpid())
    return [ln for ln in r.stdout.splitlines()
            if ln and ln.split(None, 1)[0] not in (self_pid, "bash")]


def _tail_lines(path: Path, n: int = 5) -> list[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return [ln.rstrip("\n") for ln in fh.readlines()[-n:]]
    except OSError:
        return []


def _read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        for ln in path.read_text(encoding="utf-8", errors="replace").splitlines():
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                k, v = ln.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return out


def _ro_sqlite(path: Path) -> Optional[sqlite3.Connection]:
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                               timeout=5)
    except sqlite3.Error:
        return None


def _pct(part: int, total: int) -> Optional[float]:
    if total <= 0:
        return None
    return round(part * 100.0 / total, 2)


# ---------------------------------------------------------------------------
# collectors (each returns a stage dict; never raises)
# ---------------------------------------------------------------------------

def collect_snapshot(env: dict) -> dict:
    """Extraction stage: latest snapshot row + live snapshot state file."""
    out: dict[str, Any] = {"status": "unknown"}
    db = env["data_dir"] / "db" / "corpus_sync.db"
    if not db.is_file():
        out["error"] = f"{db} not found"
        return out
    conn = _ro_sqlite(db)
    if conn is None:
        out["error"] = "corpus_sync.db unreadable"
        return out
    try:
        row = conn.execute(
            "SELECT id, created_at, status, total_docs, book_counts"
            " FROM snapshots ORDER BY created_at DESC LIMIT 1").fetchone()
        if row:
            books = json.loads(row[4] or "{}")
            out.update({
                "status": "complete" if row[2] == "complete" else "partial",
                "snap_id": row[0], "created_at": row[1],
                "total_docs": row[3], "book_count": len(books),
                "books": books,
            })
        else:
            out["error"] = "no snapshots recorded"
    except sqlite3.Error as e:
        out["error"] = str(e)[:200]
    finally:
        conn.close()
    st = env["data_dir"] / "state" / "snapshot_state.json"
    if st.is_file():
        try:
            js = json.loads(st.read_text(encoding="utf-8"))
            out["last_activity"] = js.get("last_activity")
        except (OSError, ValueError):
            pass
    if out.get("status") == "unknown":
        out.setdefault("status", "unknown")
    return out


def collect_enrichment(env: dict, total_docs_by_snap: dict) -> dict:
    """Enrichment stage: latest enrich_state.json + worker liveness."""
    out: dict[str, Any] = {"status": "idle", "workers": 0}
    base = env["data_dir"] / "out" / "enrich"
    if not base.is_dir():
        out["status"] = "unknown"
        out["error"] = f"{base} not found"
        return out
    best: Optional[tuple[float, Path]] = None
    try:
        for d in base.iterdir():
            sp = d / "enrich_state.json"
            if d.is_dir() and sp.is_file():
                m = sp.stat().st_mtime
                if best is None or m > best[0]:
                    best = (m, sp)
    except OSError:
        pass
    if best is None:
        out["status"] = "unknown"
        out["error"] = "no enrich_state.json found"
        return out
    sp = best[1]
    try:
        state = json.loads(sp.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        out["status"] = "unknown"
        out["error"] = f"enrich_state.json unreadable: {e}"[:200]
        return out
    snap_id = state.get("snap_id")
    completed = state.get("completed", [])
    failed = state.get("failed", {})
    total = total_docs_by_snap.get(snap_id) or len(completed)
    out.update({
        "snap_id": snap_id,
        "total_docs": total,
        "enriched": len(completed),
        "failed": len(failed),
        "progress_pct": _pct(len(completed), total),
        "last_activity": state.get("last_activity"),
        "workers": len(_pgrep("corpus_sync.cli enrich")),
        "state_file": str(sp),
    })
    if out["workers"] > 0:
        out["status"] = "running"
    elif len(failed) and len(completed) + len(failed) >= total:
        out["status"] = "partial"
    elif len(completed) >= total and total > 0:
        out["status"] = "complete"
    elif completed:
        out["status"] = "paused"
    # recent ERROR lines from the progress log (same dir)
    log = sp.parent / "enrich_progress.log"
    out["recent_errors"] = [ln for ln in _tail_lines(log, 200)
                            if " ERROR " in ln][-RECENT_ERROR_LINES:]
    return out


def collect_embedding(env: dict) -> dict:
    """Embedding/index stage: drag.db chunks + index_versions + log tail."""
    out: dict[str, Any] = {"status": "unknown"}
    db = env["drag_db"]
    if not db.is_file():
        out["error"] = f"{db} not found"
        return out
    conn = _ro_sqlite(db)
    if conn is None:
        out["error"] = "drag.db unreadable"
        return out
    try:
        rows = conn.execute(
            "SELECT indexed, COUNT(*) FROM chunks GROUP BY indexed").fetchall()
        total = sum(c for _, c in rows)
        embedded = sum(c for f, c in rows if f)
        ver = conn.execute(
            "SELECT version_id, status, embed_model, embed_dim, embed_version,"
            " created_at, updated_at FROM index_versions"
            " ORDER BY created_at DESC LIMIT 1").fetchone()
        out.update({
            "chunks_total": total,
            "chunks_embedded": embedded,
            "progress_pct": _pct(embedded, total),
        })
        if ver:
            out.update({
                "version": ver[0],
                "index_status": ver[1],
                "embed_model": ver[2],
                "embed_dim": ver[3],
                "embed_version": ver[4],
                "index_updated_at": ver[6],
            })
        out["supervisor_running"] = bool(_pgrep("index_supervise"))
        out["index_workers"] = len(_pgrep("drag.cli index"))
        if out["index_workers"] > 0 and embedded < total:
            out["status"] = "running"
        elif embedded >= total and total > 0:
            out["status"] = "complete"
        elif out.get("supervisor_running"):
            out["status"] = "waiting"
        elif embedded > 0:
            out["status"] = "paused"
    except sqlite3.Error as e:
        out["error"] = str(e)[:200]
    finally:
        conn.close()
    tail = _tail_lines(env["drag_log"], 6)
    out["recent_log"] = [ln for ln in tail if ln.strip()][-3:]
    return out


def collect_es(env: dict) -> dict:
    url = f"{env['es_url']}/{env['es_index']}/_count"
    try:
        code, body, ms = _http(url)
        if code == 200 and isinstance(body, dict):
            return {"status": "ok", "index": env["es_index"],
                    "docs": body.get("count"), "latency_ms": round(ms, 1)}
        return {"status": "down", "index": env["es_index"], "docs": None,
                "error": f"code={code} {body}"[:200]}
    except Exception as e:  # noqa: BLE001
        return {"status": "down", "index": env["es_index"], "docs": None,
                "error": f"{type(e).__name__}: {e}"[:200]}


def collect_neo4j(env: dict) -> dict:
    pw = env.get("neo4j_password") or ""
    if not pw and env.get("neo4j_env_file"):
        pw = _read_env_file(Path(env["neo4j_env_file"])).get("NEO4J_PASSWORD", "")
    if not pw:
        return {"status": "unknown", "error": "NEO4J_PASSWORD not available"}
    cypher = ("MATCH (n) RETURN count(n) AS n;"
              " MATCH ()-[r]->() RETURN count(r) AS r;")
    try:
        r = subprocess.run(
            ["docker", "exec", "-i", env["neo4j_container"], "cypher-shell",
             "-u", env["neo4j_user"], "-p", pw, cypher],
            capture_output=True, text=True, timeout=30)
        nums = re.findall(r"^\d{1,12}$", r.stdout, re.M)
        if r.returncode == 0 and len(nums) >= 2:
            return {"status": "ok", "nodes": int(nums[0]),
                    "relationships": int(nums[1])}
        err = (r.stderr or r.stdout or "").strip()[:200]
        return {"status": "down", "error": err or f"rc={r.returncode}"}
    except (OSError, subprocess.SubprocessError) as e:
        return {"status": "unknown",
                "error": f"{type(e).__name__}: {e}"[:200]}


def collect_rag(env: dict) -> dict:
    url = env["rag_provider_url"] + "/health"
    headers = {}
    if env.get("rag_provider_token"):
        headers["Authorization"] = f"Bearer {env['rag_provider_token']}"
    try:
        code, body, ms = _http(url, headers)
        status = {200: "ok", 401: "auth", 404: "down"}.get(code)
        if status is None:
            status = "ok" if 200 <= code < 300 else "down"
        out: dict[str, Any] = {"status": status, "url": url,
                               "latency_ms": round(ms, 1)}
        if code != 200:
            out["error"] = f"code={code} {body}"[:200]
        return out
    except Exception as e:  # noqa: BLE001
        return {"status": "down", "url": url,
                "error": f"{type(e).__name__}: {e}"[:200]}


def collect_embedder(env: dict) -> dict:
    try:
        code, body, ms = _http(env["embedder_url"])
        if code == 200:
            models = []
            if isinstance(body, dict) and isinstance(body.get("data"), list):
                models = [m.get("id") for m in body["data"]
                          if isinstance(m, dict)]
            return {"status": "ok", "url": env["embedder_url"],
                    "latency_ms": round(ms, 1), "models": models}
        return {"status": "down", "url": env["embedder_url"],
                "error": f"code={code} {body}"[:200]}
    except Exception as e:  # noqa: BLE001
        return {"status": "down", "url": env["embedder_url"],
                "error": f"{type(e).__name__}: {e}"[:200]}


def collect_hf(env: dict) -> dict:
    out: dict[str, Any] = {"repo": env["hf_repo_id"],
                           "is_private": env["hf_is_private"],
                           "status": "not_staged"}
    outdir = env["data_dir"] / "out"
    if outdir.is_dir():
        # publish.py stages: <dest>/manifests/manifest.json
        try:
            manifests = [m for m in sorted(outdir.rglob("manifest.json"))
                         if "manifests" in m.parts]
        except OSError:
            manifests = []
        if manifests:
            m = manifests[0]
            out["status"] = "staged"
            try:
                out["manifest"] = str(m.relative_to(env["data_dir"]))
            except ValueError:
                out["manifest"] = str(m)
            out["staged_at"] = datetime.fromtimestamp(
                m.stat().st_mtime, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                js = json.loads(m.read_text(encoding="utf-8"))
                if isinstance(js, dict):
                    for k in ("repo", "month", "snapshot", "created_at",
                              "docs_total", "license"):
                        if k in js:
                            out[k] = js[k]
            except (OSError, ValueError):
                pass
    if env.get("hf_token") and out["status"] != "staged":
        code, body, _ = _http(
            f"https://huggingface.co/api/datasets/{env['hf_repo_id']}")
        if code == 200:
            out["status"] = "repo_exists_remote"
        elif code == 401:
            out["status"] = "auth"
        elif code == 404:
            out["status"] = "not_published"
    return out


def collect_git(env: dict) -> dict:
    """Repo health: branch + HEAD of the deployment clone (real, not guessed)."""
    try:
        root = str(env["repo_root"])
        head = subprocess.run(["git", "-C", root, "rev-parse", "--short", "HEAD"],
                              capture_output=True, text=True, timeout=10)
        branch = subprocess.run(["git", "-C", root, "rev-parse",
                                 "--abbrev-ref", "HEAD"],
                                capture_output=True, text=True, timeout=10)
        dirty = subprocess.run(["git", "-C", root, "status", "--porcelain"],
                               capture_output=True, text=True, timeout=10)
        if head.returncode != 0:
            return {"status": "unknown",
                    "error": (head.stderr or "no git repo").strip()[:120]}
        return {"status": "ok", "head": head.stdout.strip(),
                "branch": branch.stdout.strip(),
                "dirty_files": len([l for l in dirty.stdout.splitlines()
                                    if l.strip()])}
    except (OSError, subprocess.SubprocessError) as e:
        return {"status": "unknown", "error": f"{type(e).__name__}: {e}"[:120]}


def total_docs_by_snap(env: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    db = env["data_dir"] / "db" / "corpus_sync.db"
    conn = _ro_sqlite(db) if db.is_file() else None
    if conn is not None:
        try:
            for sid, n in conn.execute(
                    "SELECT id, total_docs FROM snapshots"):
                out[sid] = n
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    return out


def build_status(env: dict, window: Optional["RateWindow"]) -> dict:
    """Assemble the full /v1/status document (all collectors in parallel)."""
    totals = total_docs_by_snap(env)
    with ThreadPoolExecutor(max_workers=10) as pool:
        f_snap = pool.submit(collect_snapshot, env)
        f_enr = pool.submit(collect_enrichment, env, totals)
        f_emb = pool.submit(collect_embedding, env)
        f_es = pool.submit(collect_es, env)
        f_n4j = pool.submit(collect_neo4j, env)
        f_rag = pool.submit(collect_rag, env)
        f_edi = pool.submit(collect_embedder, env)
        f_hf = pool.submit(collect_hf, env)
        f_git = pool.submit(collect_git, env)
        results = [f.result() for f in
                   (f_snap, f_enr, f_emb, f_es, f_n4j, f_rag, f_edi, f_hf,
                    f_git)]
    snap, enr, emb, es, n4j, rag, edi, hf, git = results

    enr_rate = window.rate_per_hour(1) if window else None
    emb_rate = window.rate_per_hour(2) if window else None

    doc = {
        "api_version": API_VERSION,
        "corpus_id": env["corpus_id"],
        "generated_at": _now(),
        "pipeline": {
            "extraction": snap,
            "enrichment": enr,
            "embedding": emb,
        },
        "stores": {
            "elasticsearch": es,
            "neo4j": n4j,
            "huggingface": hf,
        },
        "services": {
            "rag_provider": rag,
            "embedder": edi,
            "git": git,
        },
        "rates": {
            "enrichment_per_hour": enr_rate,
            "embedding_per_hour": emb_rate,
            "window_note": "computed from 60s in-memory poll window"
                           " since process start; null if < 5 min of samples",
        },
        "errors": {
            "enrichment_failed": (len(enr.get("failed", {}))
                                  if isinstance(enr.get("failed"), dict)
                                  else (enr.get("failed")
                                        if isinstance(
                                            enr.get("failed"), int) else None)),
            "recent": {
                "enrichment": enr.get("recent_errors", []),
                "embedding_log": emb.get("recent_log", []),
            },
        },
    }
    doc["health"] = _overall_health(doc)
    return doc


_SEV = {"ok": 0, "complete": 0, "partial": 1, "staged": 0,
        "repo_exists_remote": 0, "auth": 1, "paused": 1, "waiting": 1,
        "degraded": 1, "unknown": 2, "idle": 2, "not_staged": 2,
        "not_published": 2, "running": 0, "down": 3, "failed": 3}


def _overall_health(doc: dict) -> dict:
    checks: dict[str, str] = {}
    p, s, sv = doc["pipeline"], doc["stores"], doc["services"]
    checks["extraction"] = p["extraction"].get("status", "unknown")
    checks["enrichment"] = p["enrichment"].get("status", "unknown")
    checks["embedding"] = p["embedding"].get("status", "unknown")
    checks["elasticsearch"] = s["elasticsearch"].get("status", "unknown")
    checks["neo4j"] = s["neo4j"].get("status", "unknown")
    checks["rag_provider"] = sv["rag_provider"].get("status", "unknown")
    checks["embedder"] = sv["embedder"].get("status", "unknown")
    worst = max(_SEV.get(c, 2) for c in checks.values())
    overall = {0: "ok", 1: "degraded", 2: "degraded", 3: "down"}[worst]
    return {"overall": overall, "checks": checks}


# ---------------------------------------------------------------------------
# rate window + hourly history
# ---------------------------------------------------------------------------

class RateWindow:
    """Rolling 60s counter samples; derives hourly rates + history file."""

    def __init__(self, history_dir: Path) -> None:
        self.samples: deque = deque(maxlen=WINDOW_MAX)
        self.lock = threading.Lock()
        self.history_path = Path(history_dir) / "history.jsonl"
        self._last_hour: Optional[str] = None

    def add(self, enriched: int, embedded: int, es_docs: int) -> None:
        ts = time.time()
        with self.lock:
            self.samples.append((ts, enriched, embedded, es_docs))
        try:
            self._maybe_persist()
        except Exception:  # noqa: BLE001 - persistence must not kill the poller
            pass

    def rate_per_hour(self, idx: int) -> Optional[float]:
        with self.lock:
            s = list(self.samples)
        if len(s) < 5:
            return None
        now = s[-1]
        prev = min(s, key=lambda x: abs(x[0] - (now[0] - 3600)))
        if prev is now:
            prev = s[0]
        span_h = (now[0] - prev[0]) / 3600.0
        if span_h < 1 / 60.0:      # < 1 min of data
            return None
        return round((now[idx] - prev[idx]) / span_h, 1)

    def snapshot_counts(self) -> Optional[dict]:
        with self.lock:
            if not self.samples:
                return None
            ts, e, m, d = self.samples[-1]
            return {"enriched": e, "embedded": m, "es_docs": d}

    def _maybe_persist(self) -> None:
        # Called WITHOUT holding self.lock: rate_per_hour re-acquires it.
        hour = time.strftime("%Y-%m-%dT%H", time.gmtime())
        if hour == self._last_hour:
            return
        with self.lock:
            s = list(self.samples)
        if len(s) < 5:
            return
        span_min = (s[-1][0] - s[0][0]) / 60.0
        span_h = (s[-1][0] - s[0][0]) / 3600.0

        def _rate(idx: int):
            if span_h < 1 / 60.0:
                return None
            return round((s[-1][idx] - s[0][idx]) / span_h, 1)

        rec = {
            "ts": _now(),
            "window_minutes": round(span_min, 1),
            "counts": {"enriched": s[-1][1], "embedded": s[-1][2],
                       "es_docs": s[-1][3]},
            "rates_per_hour": {"enrichment": _rate(1),
                               "embedding": _rate(2)},
        }
        self._last_hour = hour
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.history_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def poll_loop(window: RateWindow, env: dict, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            totals = total_docs_by_snap(env)
            enr_n = _enriched_count(env, totals)
            emb_n = _embedded_count(env)
            es_n = _es_count(env)
            window.add(enr_n, emb_n, es_n)
        except Exception as e:  # noqa: BLE001 - keep the poller alive
            print(f"[{_now()}] poll error: {e}", flush=True)
        stop.wait(POLL_S)


def _enriched_count(env: dict, totals: dict) -> int:
    base = env["data_dir"] / "out" / "enrich"
    best: Optional[Path] = None
    if base.is_dir():
        for d in base.iterdir():
            sp = d / "enrich_state.json"
            if d.is_dir() and sp.is_file():
                if best is None or sp.stat().st_mtime > best.stat().st_mtime:
                    best = sp
    if best is None:
        return 0
    try:
        return len(json.loads(best.read_text(encoding="utf-8")).get(
            "completed", []))
    except (OSError, ValueError):
        return 0


def _embedded_count(env: dict) -> int:
    conn = _ro_sqlite(env["drag_db"]) if env["drag_db"].is_file() else None
    if conn is None:
        return 0
    try:
        r = conn.execute("SELECT COUNT(*) FROM chunks WHERE indexed=1").fetchone()
        return int(r[0]) if r else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def _es_count(env: dict) -> int:
    try:
        code, body, _ = _http(f"{env['es_url']}/{env['es_index']}/_count",
                              timeout=3)
        if code == 200 and isinstance(body, dict):
            return int(body.get("count") or 0)
    except Exception:  # noqa: BLE001
        pass
    return 0


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

def load_env() -> dict:
    data_dir = Path(os.environ.get("STATUS_DATA_DIR", "data"))
    return {
        "corpus_id": os.environ.get("STATUS_CORPUS_ID", "rewaat"),
        "data_dir": data_dir,
        "repo_root": Path(os.environ.get("STATUS_REPO_ROOT", ".")),
        "drag_db": Path(os.environ.get(
            "STATUS_DRAG_DB",
            "/home/saber/dedicated-rag/data/rewayaat/drag.db")),
        "drag_log": Path(os.environ.get(
            "STATUS_DRAG_LOG",
            "/home/saber/dedicated-rag/data/rewayaat/index_gpu2.log")),
        "es_url": os.environ.get("ES_URL", "http://127.0.0.1:9200").rstrip("/"),
        "es_index": os.environ.get("ES_INDEX", "rewayaat_corpus_v1"),
        "neo4j_container": os.environ.get("NEO4J_CONTAINER", "neo4j_db"),
        "neo4j_user": os.environ.get("NEO4J_USER", "neo4j"),
        "neo4j_password": os.environ.get("NEO4J_PASSWORD", ""),
        "neo4j_env_file": os.environ.get(
            "NEO4J_ENV_FILE", "/home/saber/saberprojects/graph-rag/.env"),
        "rag_provider_url": os.environ.get(
            "RAG_PROVIDER_URL", "http://127.0.0.1:8107").rstrip("/"),
        "rag_provider_token": os.environ.get("RAG_PROVIDER_TOKEN", ""),
        "embedder_url": os.environ.get(
            "STATUS_EMBEDDER_URL", "http://172.20.1.53:4001/v1/models"),
        "hf_repo_id": os.environ.get(
            "HF_REPO_ID", "saber13812002/rewayaat-corpus"),
        "hf_is_private": os.environ.get("HF_IS_PRIVATE", "true") == "true",
        "hf_token": os.environ.get("HF_TOKEN", ""),
    }


def make_handler(env: dict, window: RateWindow):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"CorpusStatusAPI/{API_VERSION}"

        def log_message(self, fmt, *args):  # noqa: D401 - quiet logs
            print(f"[{_now()}] {self.address_string()} {fmt % args}",
                  flush=True)

        def _send(self, code: int, obj: Any) -> None:
            body = json.dumps(obj, ensure_ascii=False, indent=1).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path != "/":
                path = path.rstrip("/")
            try:
                if path == "/":
                    self._send(200, {
                        "name": f"{env['corpus_id']}-corpus-status",
                        "api_version": API_VERSION,
                        "corpus_id": env["corpus_id"],
                        "contract": "docs/corpus-sync/corpus-status-api-v1.md",
                        "endpoints": ["/health", "/v1/status", "/v1/summary",
                                      "/v1/history", "/v1/errors"],
                    })
                elif path == "/health":
                    self._send(200, {
                        "status": "ok", "api_version": API_VERSION,
                        "corpus_id": env["corpus_id"],
                        "uptime_s": int(time.time() - STARTED_AT),
                    })
                elif path == "/v1/status":
                    self._send(200, build_status(env, window))
                elif path == "/v1/summary":
                    self._send(200, _summary(build_status(env, window)))
                elif path == "/v1/history":
                    self._send(200, _history(window, self.path))
                elif path == "/v1/errors":
                    doc = build_status(env, window)
                    self._send(200, doc["errors"])
                else:
                    self._send(404, {"error": "not_found", "path": path})
            except Exception as e:  # noqa: BLE001
                self._send(500, {"error": f"{type(e).__name__}: {e}"[:300]})

    return Handler


def _summary(doc: dict) -> dict:
    p, s, sv = doc["pipeline"], doc["stores"], doc["services"]
    enr, emb = p["enrichment"], p["embedding"]
    return {
        "api_version": doc["api_version"],
        "corpus_id": doc["corpus_id"],
        "generated_at": doc["generated_at"],
        "overall_health": doc["health"]["overall"],
        "docs": {
            "snapshot_total": p["extraction"].get("total_docs"),
            "enriched": enr.get("enriched"),
            "elasticsearch": s["elasticsearch"].get("docs"),
            "neo4j_nodes": s["neo4j"].get("nodes"),
        },
        "progress_pct": {
            "enrichment": enr.get("progress_pct"),
            "embedding": emb.get("progress_pct"),
        },
        "pipeline_status": {k: v.get("status") for k, v in p.items()},
        "rates_per_hour": doc["rates"],
        "services": {k: v.get("status") for k, v in sv.items()},
    }


def _history(window: RateWindow, query: str) -> dict:
    limit = 96
    if "?" in query:
        for part in query.split("?")[1].split("&"):
            if part.startswith("limit="):
                try:
                    limit = max(1, min(int(part.split("=")[1]), 24 * 30))
                except (ValueError, IndexError):
                    pass
    recs: list[dict] = []
    if window.history_path.is_file():
        try:
            lines = window.history_path.read_text(
                encoding="utf-8").splitlines()
            for ln in lines[-limit:]:
                try:
                    recs.append(json.loads(ln))
                except ValueError:
                    continue
        except OSError as e:
            return {"error": str(e)[:200], "records": []}
    return {"count": len(recs), "records": recs}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=os.environ.get("STATUS_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("STATUS_PORT", "8190")))
    args = ap.parse_args(argv)
    env = load_env()
    window = RateWindow(env["data_dir"] / "status")
    stop = threading.Event()
    t = threading.Thread(target=poll_loop, args=(window, env, stop),
                         name="status-poller", daemon=True)
    t.start()
    srv = ThreadingHTTPServer((args.host, args.port), make_handler(env, window))
    print(f"[{_now()}] corpus-status-api {API_VERSION} corpus="
          f"{env['corpus_id']} on http://{args.host}:{args.port} "
          f"(data_dir={env['data_dir'].resolve()})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
