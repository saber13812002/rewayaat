import pytest

from corpus_sync import http


class _FakeClock:
    """Returns 0, 0.1, 0.2, ... on successive calls."""

    def __init__(self):
        self.t = 0.0

    def __call__(self):
        t = self.t
        self.t += 0.1
        return t


fake_clock = _FakeClock()


def test_retries_then_succeeds(monkeypatch):
    calls = []

    def flaky(url, params=None):
        calls.append(url)
        if len(calls) < 3:
            raise http.TransportError("boom")
        return b'{"ok": true}'

    monkeypatch.setattr(http, "_do_request", flaky)
    monkeypatch.setattr(http, "_sleep", lambda s: None)
    assert http.get_json("https://x/y") == {"ok": True}
    assert len(calls) == 3


def test_raises_httpererror_after_max_retries(monkeypatch):
    monkeypatch.setattr(http, "_do_request", lambda u, p=None: (_ for _ in ()).throw(http.TransportError("down")))
    monkeypatch.setattr(http, "_sleep", lambda s: None)
    with pytest.raises(http.HttpError) as e:
        http.get_json("https://x/y")
    assert e.value.attempts == http.MAX_RETRIES


def test_404_is_not_retried(monkeypatch):
    calls = []

    def gone(url, params=None):
        calls.append(url)
        raise http.HttpStatusError(404, b"not found")

    monkeypatch.setattr(http, "_do_request", gone)
    monkeypatch.setattr(http, "_sleep", lambda s: None)
    with pytest.raises(http.HttpError) as e:
        http.get_json("https://x/y")
    assert e.value.status == 404 and len(calls) == 1  # 4xx (except 429/408) no retry


def test_rate_limit_spacing(monkeypatch):
    sleeps = []
    monkeypatch.setattr(http, "_do_request", lambda u, p=None: b"{}")
    monkeypatch.setattr(http, "_sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(http.time, "monotonic", fake_clock)
    http.get_json("https://x/1")
    http.get_json("https://x/2")
    assert any(s >= 0.4 for s in sleeps)  # 2 req/s => >=0.5s spacing minus epsilon


def test_backoff_is_exponential_and_increasing(monkeypatch):
    sleeps = []
    monkeypatch.setattr(http, "_do_request", lambda u, p=None: (_ for _ in ()).throw(http.TransportError("down")))
    monkeypatch.setattr(http, "_sleep", lambda s: sleeps.append(s))
    with pytest.raises(http.HttpError):
        http.get_json("https://x/y")
    # Token-bucket sleeps (~0.5s) also go through _sleep; backoff sleeps are
    # the ones >= MIN_BACKOFF_S: 5s, 10s, 20s (+jitter <1s), strictly increasing.
    backoff = [s for s in sleeps if s >= http.MIN_BACKOFF_S]
    assert len(backoff) == 3
    assert backoff[0] >= http.MIN_BACKOFF_S
    assert backoff[1] > backoff[0]
    assert backoff[2] > backoff[1]
