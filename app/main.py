"""FastAPI entrypoint: paired-end FASTQ receiver (single process)."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Path, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import config
from .manager import BatchManager

manager = BatchManager(config.DATA_DIR, config.MAX_FILE_BYTES)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Recovery runs to completion here: unfinished batches are marked FAILED
    # and reclaimed, passed batches are kept, quota accounting is rebuilt —
    # all before the server accepts the first request.
    manager.recover()
    yield


app = FastAPI(
    title="Paired-end FASTQ Receiver",
    version="1.0.0",
    description=(
        "Single-process receiving API in front of the alignment pipeline. "
        "One batch per sample; R1/R2 are each streamed once as the request "
        "body (single-member gzip of 4-line FASTQ). Both ends are validated "
        "and paired before the expanded FASTQ files can be downloaded. "
        "Interactive docs: /docs"
    ),
    lifespan=lifespan,
)


class CreateBatchRequest(BaseModel):
    sample_name: str = Field(
        ..., examples=["SAMPLE001"],
        description="1-128 chars of [A-Za-z0-9._-], starting with a letter or digit",
    )


@app.get("/api/health", tags=["meta"], summary="Liveness probe")
async def health():
    return {"status": "ok"}


@app.get("/api/quota", tags=["meta"], summary="Shared file-byte quota usage")
async def quota():
    return manager.quota.snapshot()


@app.post(
    "/api/batches", status_code=201, tags=["batches"],
    summary="Create a batch for one sample",
)
async def create_batch(req: CreateBatchRequest):
    return manager.create_batch(req.sample_name)


@app.get("/api/batches", tags=["batches"], summary="List all batches")
async def list_batches():
    return [manager.view(b) for b in manager.db.list()]


@app.get("/api/batches/{batch_id}", tags=["batches"], summary="Batch status")
async def get_batch(batch_id: str):
    return manager.get_view(batch_id)


@app.put(
    "/api/batches/{batch_id}/{end}", tags=["upload"],
    summary="Stream-upload one end (request body = single-member .fq.gz)",
)
async def upload_end(
    request: Request,
    batch_id: str,
    end: str = Path(pattern="^(r1|r2)$", description="which read end"),
):
    return await manager.upload_end(batch_id, end, request)


@app.post(
    "/api/batches/{batch_id}/cancel", tags=["batches"],
    summary="Cancel the batch (confirmed only after writes stopped, files "
            "deleted and quota released)",
)
async def cancel_batch(batch_id: str):
    return await manager.cancel(batch_id)


@app.delete(
    "/api/batches/{batch_id}", status_code=204, tags=["batches"],
    summary="Delete a finished batch (releases quota held by a PASSED batch)",
)
async def delete_batch(batch_id: str):
    await manager.delete_batch(batch_id)


@app.get(
    "/api/batches/{batch_id}/{end}.fastq", tags=["download"],
    summary="Download the expanded FASTQ (PASSED batches only)",
)
async def download_end(
    batch_id: str,
    end: str = Path(pattern="^(r1|r2)$", description="which read end"),
):
    b = manager.db.get(batch_id)
    if b is None:
        raise HTTPException(404, "batch not found")
    if b["status"] != "PASSED":
        raise HTTPException(
            409, f"batch is {b['status']}; downloads require PASSED"
        )
    path = manager.batch_dir(batch_id) / f"{end}.fastq"
    if not path.is_file():
        raise HTTPException(410, "expanded FASTQ is gone")
    return FileResponse(
        path, media_type="text/plain",
        filename=f"{b['sample_name']}_{end}.fastq",
    )
