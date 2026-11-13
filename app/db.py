"""SQLite state store (single process, WAL)."""
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id    TEXT PRIMARY KEY,
    sample_name TEXT NOT NULL,
    status      TEXT NOT NULL,                -- RECEIVING/VALIDATING/PASSED/FAILED/CANCELLED
    error       TEXT,
    r1_state    TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING/RECEIVED/VALIDATED
    r2_state    TEXT NOT NULL DEFAULT 'PENDING',
    r1_bytes    INTEGER NOT NULL DEFAULT 0,   -- expanded FASTQ bytes
    r2_bytes    INTEGER NOT NULL DEFAULT 0,
    read_count  INTEGER,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
"""

TERMINAL = ("PASSED", "FAILED", "CANCELLED")
ACTIVE = ("RECEIVING", "VALIDATING")


class DB:
    def __init__(self, path):
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)

    def create(self, batch_id: str, sample_name: str) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO batches(batch_id, sample_name, status, created_at, updated_at)"
                " VALUES(?,?,?,?,?)",
                (batch_id, sample_name, "RECEIVING", now, now),
            )
            self._conn.commit()

    def get(self, batch_id: str):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
        return dict(row) if row else None

    def list(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def update(self, batch_id: str, **fields) -> None:
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._lock:
            self._conn.execute(
                f"UPDATE batches SET {cols} WHERE batch_id=?",
                (*fields.values(), batch_id),
            )
            self._conn.commit()

    def cas_status(self, batch_id: str, old: str, new: str) -> bool:
        """Compare-and-swap the batch status; True if this call won."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE batches SET status=?, updated_at=? WHERE batch_id=? AND status=?",
                (new, time.time(), batch_id, old),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def delete(self, batch_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM batches WHERE batch_id=?", (batch_id,))
            self._conn.commit()
