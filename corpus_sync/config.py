"""Central configuration for the corpus sync pipeline.

All tunables live here so that tuning (rate limits, timeouts, paths) never
requires touching call sites. See docs/corpus-sync/recovery-guide.md.
"""

from pathlib import Path

# --- API -------------------------------------------------------------------
BASE_URL: str = "https://rewayaat.info"
PER_PAGE: int = 200                 # LOOKUP page size (server default is 20)
REQUEST_TIMEOUT_S: int = 240        # live API variance: same query 1.2s..>120s
MAX_RETRIES: int = 4                # attempts per request (1 + 3 retries)
MIN_BACKOFF_S: float = 5.0          # exponential base: 5s, 10s, 20s, 40s
RATE_LIMIT_PER_S: float = 2.0       # sustained request rate (token bucket)

# --- Local paths (working dir; gitignored) ----------------------------------
DATA_DIR: Path = Path("data")
DB_PATH: Path = DATA_DIR / "db" / "corpus_sync.db"
RAW_DIR: Path = DATA_DIR / "raw"
STATE_DIR: Path = DATA_DIR / "state"
OUT_DIR: Path = DATA_DIR / "out"

# --- Normalization -----------------------------------------------------------
NORM_VERSION: str = "1.0"           # rules v1.0: NFKC + strip for field hashes

# --- Hugging Face ------------------------------------------------------------
HF_REPO_ID: str = "saber13812002/rewayaat-corpus"
HF_IS_PRIVATE: bool = True          # stays private until a data license is confirmed
