"""Resilient HTTP client for the rewayaat.info REST API.

Observed live (2026-11-15): identical queries took 1.2 s and then >120 s,
and DNS resolution failed transiently (socket.gaierror 11001). Hence:
- per-request timeout of 240 s
- up to 4 attempts with exponential backoff + jitter
- sustained rate limited to 2 req/s (token bucket, single connection)

Retry policy: transport errors, 429, 408, and 5xx are retried; other 4xx
(e.g. 404) are not — they are client errors and will not heal on their own.
"""

import json
import random
import time

import requests

from . import config

# Re-export the tuning knobs this module uses, so callers and tests can read
# them in one place (config remains the single source of truth).
MAX_RETRIES = config.MAX_RETRIES
MIN_BACKOFF_S = config.MIN_BACKOFF_S
RATE_LIMIT_PER_S = config.RATE_LIMIT_PER_S


class TransportError(Exception):
    """Network-level failure (timeout, DNS, connection reset)."""


class HttpStatusError(Exception):
    """Server answered with status >= 400."""

    def __init__(self, status: int, body: bytes):
        super().__init__(f"HTTP {status}")
        self.status = status
        # Keep a short body for 5xx diagnostics; never log the full body.
        self.body = body[:200]


class HttpError(Exception):
    """Raised when a request still fails after MAX_RETRIES attempts."""

    def __init__(self, url: str, attempts: int, status: int | None = None, cause: Exception | None = None):
        detail = f"HTTP {status}" if status is not None else cause.__class__.__name__
        super().__init__(f"GET {url} failed after {attempts} attempts ({detail})")
        self.url = url
        self.attempts = attempts
        self.status = status


_session = requests.Session()
_session.headers["User-Agent"] = "rewayaat-corpus-sync/1.0"

# Token-bucket state (module level so tests can reset it via conftest).
_last_request_at: float | None = None


def _do_request(url: str, params: dict | None = None) -> bytes:
    """One GET attempt. Returns body bytes.

    Raises TransportError on requests.RequestException and
    HttpStatusError(status, body) on status >= 400.
    """
    try:
        r = _session.get(url, params=params, timeout=config.REQUEST_TIMEOUT_S)
    except requests.RequestException as e:
        raise TransportError(str(e)) from e
    if r.status_code >= 400:
        raise HttpStatusError(r.status_code, r.content)
    return r.content


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _retryable(exc: Exception) -> tuple[bool, int | None]:
    if isinstance(exc, TransportError):
        return True, None
    if isinstance(exc, HttpStatusError):
        status = exc.status
        return status in (408, 429) or status >= 500, status
    return False, None


def _request(url: str, params: dict | None = None) -> bytes:
    """GET with retries, backoff, and rate limiting. Returns body bytes."""
    global _last_request_at
    attempt = 0
    while True:
        # Token bucket: keep sustained rate <= RATE_LIMIT_PER_S.
        # (Goes through _sleep like backoff, so tests can observe both.)
        now = time.monotonic()
        if _last_request_at is not None:
            gap = now - _last_request_at
            min_gap = 1.0 / config.RATE_LIMIT_PER_S
            if gap < min_gap:
                _sleep(min_gap - gap)
        _last_request_at = time.monotonic()

        attempt += 1
        try:
            return _do_request(url, params)
        except Exception as exc:  # noqa: BLE001 - classified below
            retryable, status = _retryable(exc)
            if not retryable or attempt >= config.MAX_RETRIES:
                raise HttpError(url, attempt, status=status, cause=exc) from exc
            backoff = min(60.0, config.MIN_BACKOFF_S * 2 ** (attempt - 1))
            _sleep(backoff + random.uniform(0.0, 1.0))


def get_json(url: str, *, params: dict | None = None) -> object:
    """GET and parse JSON. Raises HttpError after MAX_RETRIES."""
    return json.loads(_request(url, params).decode("utf-8"))


def get_text(url: str, *, params: dict | None = None) -> str:
    """GET and decode text. Raises HttpError after MAX_RETRIES."""
    return _request(url, params).decode("utf-8")
