"""Batch lifecycle: uploads, validation, cancellation, termination, recovery."""
from __future__ import annotations

import asyncio
import re
import shutil
import uuid
from pathlib import Path

import anyio
from fastapi import HTTPException
from starlette.requests import ClientDisconnect

from .db import DB, TERMINAL
from .fastq import Cancelled, FastqError, compare_pairs, decompress_validate_fastq
from .quota import Quota, QuotaExceeded
from .runtime import Runtimes

SAMPLE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ENDS = ("r1", "r2")


class BatchManager:
    def __init__(self, data_dir: Path, max_file_bytes: int):
        self.data_dir = Path(data_dir)
        self.batches_dir = self.data_dir / "batches"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.batches_dir.mkdir(parents=True, exist_ok=True)
        self.db = DB(self.data_dir / "receiver.db")
        self.quota = Quota(max_file_bytes)
        self.runtimes = Runtimes()

    # ------------------------------------------------------------------ paths
    def batch_dir(self, batch_id: str) -> Path:
        return self.batches_dir / batch_id

    # ------------------------------------------------------- startup recovery
    def recover(self) -> None:
        """Reconcile DB state with the data volume before accepting traffic.

        Passed batches are kept (and keep counting against the quota);
        unfinished batches are marked FAILED and their files reclaimed;
        orphan directories are removed. Quota accounting is rebuilt from
        what actually remains on disk. Runs to completion during startup,
        before the server accepts requests.
        """
        for b in self.db.list():
            d = self.batch_dir(b["batch_id"])
            if b["status"] in ("RECEIVING", "VALIDATING"):
                self.db.update(
                    b["batch_id"], status="FAILED",
                    error="interrupted by service restart",
                )
                self._remove_tree(d)
            elif b["status"] == "PASSED":
                if not all((d / f"{e}.fastq").is_file() for e in ENDS):
                    self.db.update(
                        b["batch_id"], status="FAILED",
                        error="expanded FASTQ missing after restart",
                    )
                    self._remove_tree(d)
            else:  # FAILED / CANCELLED must not keep files
                self._remove_tree(d)
        known = {b["batch_id"] for b in self.db.list()}
        for d in self.batches_dir.iterdir():
            if d.is_dir() and d.name not in known:
                shutil.rmtree(d, ignore_errors=True)
        total = sum(
            p.stat().st_size for p in self.batches_dir.rglob("*") if p.is_file()
        )
        self.quota.reset(total)

    # ------------------------------------------------- file helpers (sync)
    def _remove_tree(self, d: Path) -> int:
        total = 0
        if d.is_dir():
            for p in d.rglob("*"):
                if p.is_file():
                    try:
                        total += p.stat().st_size
                    except OSError:
                        pass
            shutil.rmtree(d, ignore_errors=True)
        if total:
            self.quota.release(total)
        return total

    def _remove_file(self, p: Path) -> None:
        try:
            size = p.stat().st_size
        except OSError:
            return
        try:
            p.unlink()
        except OSError:
            return
        self.quota.release(size)

    # -------------------------------------------------------------- views
    def view(self, b: dict) -> dict:
        rt = self.runtimes.get(b["batch_id"])
        ends = {}
        for e in ENDS:
            ends[e] = {
                "state": b[f"{e}_state"],
                "received_bytes": (rt.progress[e] if rt else 0) or b[f"{e}_bytes"],
                "expanded_bytes": b[f"{e}_bytes"],
            }
        return {
            "batch_id": b["batch_id"],
            "sample_name": b["sample_name"],
            "status": b["status"],
            "error": b["error"],
            "read_count": b["read_count"],
            "ends": ends,
            "created_at": b["created_at"],
            "updated_at": b["updated_at"],
        }

    def get_view(self, batch_id: str) -> dict:
        b = self.db.get(batch_id)
        if b is None:
            raise HTTPException(404, "batch not found")
        return self.view(b)

    # ------------------------------------------------------------- create
    def create_batch(self, sample_name: str) -> dict:
        if not SAMPLE_RE.match(sample_name or ""):
            raise HTTPException(
                422,
                "invalid sample_name: 1-128 chars of [A-Za-z0-9._-], "
                "starting with a letter or digit",
            )
        batch_id = uuid.uuid4().hex
        self.db.create(batch_id, sample_name)
        self.runtimes.add(batch_id)
        return self.get_view(batch_id)

    # ------------------------------------------------------------- upload
    async def upload_end(self, batch_id: str, end: str, request) -> dict:
        b = self.db.get(batch_id)
        if b is None:
            raise HTTPException(404, "batch not found")
        if b["status"] != "RECEIVING":
            raise HTTPException(
                409, f"batch is {b['status']}; uploads not accepted"
            )
        if b[f"{end}_state"] != "PENDING":
            raise HTTPException(409, f"{end} already uploaded")
        rt = self.runtimes.get(batch_id)
        if rt is None:
            raise HTTPException(409, "batch is not accepting uploads")
        async with rt.cond:
            if rt.cancel.is_set():
                raise HTTPException(409, "batch is being terminated")
            if rt.end_uploading[end]:
                raise HTTPException(409, f"{end} upload already in progress")
            rt.end_uploading[end] = True
            rt.active += 1

        d = self.batch_dir(batch_id)
        tmp = d / f"{end}.fq.gz"
        received = 0
        failure = None  # ("cancelled"|"quota"|"error", exception)
        try:
            await anyio.to_thread.run_sync(
                lambda: d.mkdir(parents=True, exist_ok=True)
            )
            f = open(tmp, "wb")
            try:
                async for chunk in request.stream():
                    if rt.cancel.is_set():
                        raise Cancelled()
                    self.quota.acquire(len(chunk))   # limit enforced while receiving
                    await anyio.to_thread.run_sync(f.write, chunk)
                    received += len(chunk)
                    rt.progress[end] = received
            finally:
                f.close()
            if rt.cancel.is_set():
                raise Cancelled()
            if received == 0:
                raise FastqError("empty request body")
            self.db.update(batch_id, **{f"{end}_state": "RECEIVED"})
        except Cancelled:
            failure = ("cancelled", None)
        except QuotaExceeded as e:
            failure = ("quota", e)
        except (ClientDisconnect, FastqError, OSError) as e:
            failure = ("error", e)

        if failure is not None:
            await anyio.to_thread.run_sync(self._remove_file, tmp)
        # Unregister this writer *before* any termination so _terminate's
        # wait for active == 0 cannot deadlock on us.
        async with rt.cond:
            rt.active -= 1
            rt.end_uploading[end] = False
            rt.cond.notify_all()

        if failure is not None:
            kind, exc = failure
            if kind == "cancelled":
                raise HTTPException(409, "batch terminated during upload")
            if kind == "quota":
                await self._terminate(batch_id, "FAILED", f"{end} upload: {exc}")
                raise HTTPException(413, str(exc))
            await self._terminate(batch_id, "FAILED", f"{end} upload failed: {exc}")
            raise HTTPException(400, f"upload failed: {exc}")

        # Both .gz received -> batch moves to VALIDATING.
        b = self.db.get(batch_id)
        if b and b["r1_state"] != "PENDING" and b["r2_state"] != "PENDING":
            self.db.cas_status(batch_id, "RECEIVING", "VALIDATING")
        asyncio.create_task(self._bg_process_end(batch_id, end))
        return {
            "batch_id": batch_id,
            "end": end,
            "received_bytes": received,
            "end_state": "RECEIVED",
        }

    # ---------------------------------------------- background validation
    async def _bg_process_end(self, batch_id: str, end: str) -> None:
        rt = self.runtimes.get(batch_id)
        if rt is None or rt.cancel.is_set():
            return
        async with rt.cond:
            if rt.cancel.is_set():
                return
            rt.active += 1
        failure = None
        try:
            d = self.batch_dir(batch_id)
            src, dst = d / f"{end}.fq.gz", d / f"{end}.fastq"
            try:
                await anyio.to_thread.run_sync(
                    decompress_validate_fastq, src, dst, self.quota,
                    rt.cancel.is_set,
                )
            except Cancelled:
                return
            except (FastqError, QuotaExceeded, OSError) as e:
                failure = f"{end}: {e}"
                return
            try:
                size = dst.stat().st_size
            except OSError:
                return  # batch directory already reclaimed
            self.db.update(
                batch_id, **{f"{end}_state": "VALIDATED", f"{end}_bytes": size}
            )
            async with rt.pair_lock:
                b = self.db.get(batch_id)
                if (
                    b
                    and b["status"] == "VALIDATING"
                    and b["r1_state"] == "VALIDATED"
                    and b["r2_state"] == "VALIDATED"
                ):
                    failure = await self._pair_check(batch_id, rt)
        finally:
            async with rt.cond:
                rt.active -= 1
                rt.cond.notify_all()
            if failure is not None:
                await self._terminate(batch_id, "FAILED", failure)

    async def _pair_check(self, batch_id: str, rt) -> str | None:
        """Returns an error message on pair-check failure, None otherwise.

        Never terminates the batch itself: the caller is still a registered
        writer and must unregister before _terminate waits for active == 0.
        """
        d = self.batch_dir(batch_id)
        try:
            count = await anyio.to_thread.run_sync(
                compare_pairs, d / "r1.fastq", d / "r2.fastq", rt.cancel.is_set
            )
        except Cancelled:
            return None
        except (FastqError, OSError) as e:
            return str(e)
        self.db.update(batch_id, status="PASSED", read_count=count, error=None)
        return None

    # --------------------------------------------------------- termination
    async def _terminate(self, batch_id: str, status: str, error: str) -> None:
        """Stop all writers of the batch, delete its files, release the
        quota, and only then record the final state. Idempotent."""
        rt = self.runtimes.get(batch_id)
        if rt is None:
            return
        async with rt.term_lock:
            if self.runtimes.get(batch_id) is not rt:
                return  # another termination already finished
            rt.cancel.set()
            async with rt.cond:
                await rt.cond.wait_for(lambda: rt.active == 0)
            # No writer of this batch can touch the disk any more.
            await anyio.to_thread.run_sync(self._remove_tree, self.batch_dir(batch_id))
            self.db.update(batch_id, status=status, error=error)
            self.runtimes.pop(batch_id)

    async def cancel(self, batch_id: str) -> dict:
        b = self.db.get(batch_id)
        if b is None:
            raise HTTPException(404, "batch not found")
        if b["status"] in TERMINAL:
            raise HTTPException(409, f"batch already {b['status']}")
        await self._terminate(batch_id, "CANCELLED", "cancelled by user")
        return {
            "batch_id": batch_id,
            "status": "CANCELLED",
            "detail": "writes stopped, files deleted, quota released",
        }

    async def delete_batch(self, batch_id: str) -> None:
        b = self.db.get(batch_id)
        if b is None:
            raise HTTPException(404, "batch not found")
        if b["status"] not in TERMINAL:
            raise HTTPException(409, "batch is still active; cancel it first")
        if b["status"] == "PASSED":
            await anyio.to_thread.run_sync(
                self._remove_tree, self.batch_dir(batch_id)
            )
        self.db.delete(batch_id)
