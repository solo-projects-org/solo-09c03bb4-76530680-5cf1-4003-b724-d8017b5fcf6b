"""Process-wide file-byte quota shared by temp uploads and expanded FASTQ."""
import threading


class QuotaExceeded(Exception):
    """Raised when a write would push stored file bytes past the limit."""


class Quota:
    """Accounting of file bytes stored in the data volume.

    Bytes are acquired *before* each write hits the disk (never after the
    whole payload landed) and released when files are deleted. Files of
    passed batches keep counting until the batch is deleted.
    """

    def __init__(self, limit_bytes: int):
        self.limit = limit_bytes
        self._used = 0
        self._lock = threading.Lock()

    def acquire(self, n: int) -> None:
        """Reserve n bytes before writing them. Raises QuotaExceeded."""
        if n <= 0:
            return
        with self._lock:
            if self._used + n > self.limit:
                raise QuotaExceeded(
                    f"file byte quota exceeded: limit={self.limit} bytes, "
                    f"in use={self._used}, requested={n}"
                )
            self._used += n

    def release(self, n: int) -> None:
        if n <= 0:
            return
        with self._lock:
            self._used = max(0, self._used - n)

    def reset(self, used: int) -> None:
        with self._lock:
            self._used = max(0, used)

    @property
    def used(self) -> int:
        with self._lock:
            return self._used

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "limit_bytes": self.limit,
                "used_bytes": self._used,
                "free_bytes": self.limit - self._used,
            }
