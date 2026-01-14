# noqa: D100
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent
DATA_DIR = ROOT_DIR / "data"

# TODO: move to /data/ folder, but then have to move the file from local to airflow container
DATA_CATALOG_PATH = ROOT_DIR / "src/data_catalog.yaml"

# =============================================================================
# Download Settings
# =============================================================================

# Chunk size for streaming downloads (in bytes)
DOWNLOAD_CHUNK_SIZE: int = 1024 * 1024  # 1 MB

# Timeout settings (in seconds)
DOWNLOAD_TIMEOUT_TOTAL: int = 600  # max time for entire download
DOWNLOAD_TIMEOUT_CONNECT: int = 10  # max time to establish connection
DOWNLOAD_TIMEOUT_SOCK_READ: int = 30  # max time between data packets

# =============================================================================
# Retry Settings
# =============================================================================

# Maximum number of retry attempts for failed operations
RETRY_MAX_ATTEMPTS: int = 3

# Initial delay between retries (in seconds)
RETRY_INITIAL_DELAY: float = 1

# Multiplier applied to delay after each failed attempt
RETRY_BACKOFF_FACTOR: float = 2.0
