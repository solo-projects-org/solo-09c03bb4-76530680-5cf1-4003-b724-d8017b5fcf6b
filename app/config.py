"""Runtime configuration (environment driven)."""
import os
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# Directory that holds the SQLite state file and all batch files (the data volume).
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data")).resolve()

# Shared file-byte quota for upload temp files AND expanded FASTQ files.
# Files of passed batches keep counting against the budget. Default 64 MiB.
MAX_FILE_BYTES = _int_env("MAX_FILE_BYTES", 64 * 1024 * 1024)

# Streaming chunk size for uploads / decompression.
CHUNK_SIZE = 256 * 1024
