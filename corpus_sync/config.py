"""Central configuration for the corpus sync pipeline.

All tunables live here so that tuning (rate limits, timeouts, paths) never
requires touching call sites. See docs/corpus-sync/recovery-guide.md.
"""

import os
from pathlib import Path

# --- API -------------------------------------------------------------------
BASE_URL: str = "https://rewayaat.info"
PER_PAGE: int = 200                 # LOOKUP page size (server default is 20)
REQUEST_TIMEOUT_S: int = 240        # live API variance: same query 1.2s..>120s
MAX_RETRIES: int = 4                # attempts per request (1 + 3 retries)
MIN_BACKOFF_S: float = 5.0          # exponential base: 5s, 10s, 20s, 40s
RATE_LIMIT_PER_S: float = 2.0       # sustained request rate (token bucket)
SNAPSHOT_PAGE_RETRIES: int = 3      # page-level attempts beyond the HTTP layer
SNAPSHOT_PAGE_BACKOFF_S: float = 15.0  # base for page-level backoff (15s, 30s, 60s)

# --- Local paths (working dir; gitignored) ----------------------------------
DATA_DIR: Path = Path("data")
DB_PATH: Path = DATA_DIR / "db" / "corpus_sync.db"
RAW_DIR: Path = DATA_DIR / "raw"
STATE_DIR: Path = DATA_DIR / "state"
OUT_DIR: Path = DATA_DIR / "out"

# --- Chapter split fixture ----------------------------------------------------
# Committed fixture with per-chapter lists + number seed sets for the books
# whose live count reaches LOOKUP's 10,000 max_result_window. Refresh it from
# a complete snapshot run when the corpus changes (see the fixture header).
SPLIT_FIXTURE: Path = Path("tests/fixtures/chapter_split.json")

# --- Normalization -----------------------------------------------------------
NORM_VERSION: str = "1.0"           # rules v1.0: NFKC + strip for field hashes

# --- Hugging Face ------------------------------------------------------------
HF_REPO_ID: str = "saber13812002/rewayaat-corpus"
HF_IS_PRIVATE: bool = True          # stays private until a data license is confirmed
HF_LICENSE: str = "UNCONFIRMED - private"

# --- LLM gateway (Phase 3 enrichment) -----------------------------------------
# OpenAI-compatible litellm gateway (verified on s52, 2026-11-21). The key is
# NOT committed: it comes from the rag-gateway container env on the server
# (GATEWAY_LLM_API_KEY == LITELLM_MASTER_KEY, len 67, prefix sk-).
LLM_BASE_URL: str = os.environ.get("LLM_BASE_URL", "http://127.0.0.1:4101")
LLM_API_KEY: str = os.environ.get("LLM_API_KEY", "")
LLM_CHAT_MODEL: str = os.environ.get("LLM_CHAT_MODEL", "gpt-oss-120b")
# gpt-oss-120b is a vLLM reasoning model: it spends completion budget on
# reasoning_content first. max_tokens=40 returned content=null with
# finish_reason=length; max_tokens=600 returned content at 136 tokens / 0.7 s.
# Always request a generous completion budget and check finish_reason.
LLM_MAX_TOKENS: int = int(os.environ.get("LLM_MAX_TOKENS", "2048"))
LLM_TEMPERATURE: float = float(os.environ.get("LLM_TEMPERATURE", "0"))
LLM_REQUEST_TIMEOUT_S: int = int(os.environ.get("LLM_REQUEST_TIMEOUT_S", "120"))
LLM_MAX_RETRIES: int = 3            # attempts per call (1 + 2 retries)
LLM_MIN_BACKOFF_S: float = 2.0
# Embedding models (verified dims): q1-embedding-4b=2560, q1-embedding-06b=1024.
LLM_EMBED_MODEL: str = os.environ.get("LLM_EMBED_MODEL", "q1-embedding-06b")
ENRICH_PROMPT_VERSION: str = "1.0"  # bump when the prompt text changes
