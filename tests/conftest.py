"""Shared pytest fixtures for the corpus_sync test suite.

- Puts the repo root on sys.path so `import corpus_sync` works from any CWD.
- Provides a tmp DATA_DIR per test (config.DATA_DIR is patched in tests that
  need on-disk state).
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _reset_http_state():
    """Reset the token-bucket clock between tests."""
    from corpus_sync import http

    http._last_request_at = None
    yield


@pytest.fixture
def tmp_data_dir(tmp_path, monkeypatch):
    """Point corpus_sync.config.DATA_DIR (and derived paths) at a temp dir."""
    from corpus_sync import config

    data_dir = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "DB_PATH", data_dir / "db" / "corpus_sync.db")
    monkeypatch.setattr(config, "RAW_DIR", data_dir / "raw")
    monkeypatch.setattr(config, "STATE_DIR", data_dir / "state")
    monkeypatch.setattr(config, "OUT_DIR", data_dir / "out")
    return data_dir
