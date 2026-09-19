"""
Great Sage — AI/Document Intelligence Service

FastAPI application providing:
  POST /api/v1/analyze  — Legacy endpoint for Poneglyph
  POST /api/v2/jobs     — Batch processing API
  GET  /health          — Health check (Tesseract + Ollama reachability)
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, status, Depends
from fastapi.responses import JSONResponse
from sqlmodel import Session, select

from app.auth import verify_api_key
from app.config import Settings, load_settings
from app.llm import check_ollama
from app.ocr import check_tesseract, is_allowed_file
from app.schemas import AcceptedResponse, HealthResponse
from app.worker import Worker
from app.database import init_db, get_session, engine
from app.routers import jobs
from app.models import Job, JobEvent
from app.queue_manager import ensure_queue_dirs
from app.queue_watcher import QueueWatcher

logger = logging.getLogger("great_sage")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Crash recovery via SQLite
# ---------------------------------------------------------------------------

def recover_stranded_jobs(worker: Worker) -> None:
    """
    On startup, reset any jobs stuck in 'processing' from a previous crashed run.

    Their directories still exist in intake/ — the QueueWatcher will detect
    them if they are re-added to the internal channel.  We reset status to
    'pending' and push them onto the channel directly.
    """
    with Session(engine) as session:
        stranded = session.exec(
            select(Job).where(Job.status == "processing")
        ).all()

        if not stranded:
            return

        for job in stranded:
            job.status = "pending"
            job.updated_at = _utcnow()
            ev = JobEvent(
                job_id=job.id,
                event="recovered",
                file_status="pending",
                source=job.source,
                detail="Reset from 'processing' on startup (crash recovery)",
                timestamp=_utcnow(),
            )
            session.add(job)
            session.add(ev)
            worker.push_to_channel(str(job.id))

        session.commit()
        logger.info(
            "Crash recovery: %d stranded job(s) reset to pending: %s",
            len(stranded), [str(j.id) for j in stranded],
        )


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle for the worker and queue watcher."""
    settings: Settings = app.state.settings
    init_db()

    # ── Queue directories ──
    ensure_queue_dirs(settings.queue_dir)

    # ── Worker pool ──
    worker = Worker(settings)
    app.state.worker = worker
    await worker.start()

    # ── Crash recovery: jobs stuck in 'processing' from a previous run ──
    recover_stranded_jobs(worker)

    # ── Filesystem watcher ──
    loop = asyncio.get_event_loop()
    watcher = QueueWatcher(
        settings=settings,
        queue=worker._queue,
        loop=loop,
    )
    watcher.start()
    app.state.watcher = watcher

    logger.info("Great Sage is ready")
    yield

    watcher.stop()
    await worker.stop()
    logger.info("Great Sage shut down")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Application factory. Accepts an optional Settings override for testing."""
    if settings is None:
        settings = load_settings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    problems = settings.validate()
    if problems:
        for p in problems:
            logger.warning("Configuration issue: %s", p)

    app = FastAPI(
        title="Great Sage",
        description="AI/Document Intelligence Service",
        version="2.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings

    app.include_router(jobs.router)

    # ── Routes ────────────────────────────────────────────────────────────

    @app.post(
        "/api/v1/analyze",
        response_model=AcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        summary="Submit a document for analysis (Legacy)",
    )
    async def analyze(
        request: Request,
        file: UploadFile = File(...),
        document_id: int = Form(...),
        session: Session = Depends(get_session)
    ):
        """
        Accept a document file (PDF, PNG, JPG) for asynchronous processing.
        Returns HTTP 202 immediately. The result is delivered via webhook.
        """
        settings: Settings = request.app.state.settings

        # ── Auth ──
        if settings.great_sage_api_key:
            verify_api_key(
                settings,
                x_api_key=request.headers.get("X-API-Key"),
                authorization=request.headers.get("Authorization"),
            )

        # ── Validate filename extension ──
        if not file.filename or not is_allowed_file(file.filename):
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Unsupported file type. Only PDF, PNG, JPG, and JPEG are accepted.",
            )

        # ── Read file with size limit ──
        content = await file.read()
        if len(content) > settings.max_upload_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File exceeds maximum size of {settings.max_upload_bytes // (1024 * 1024)} MB",
            )
        if len(content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty",
            )

        # ── Determine source ──
        source = request.headers.get("X-Source", "poneglyph:8080")

        # ── Create job in SQLite ──
        job = Job(
            legacy_document_id=document_id,
            source=source,
            status="pending",
        )
        session.add(job)
        session.commit()
        session.refresh(job)

        # ── Create JobFile record ──
        # filepath points to where it will land after queue_manager writes it
        intake_path = f"{settings.queue_dir}/intake/{job.id}/{file.filename}"
        from app.models import JobFile
        job_file = JobFile(
            job_id=job.id,
            filename=file.filename,
            filepath=intake_path,
        )
        session.add(job_file)

        # ── Write enqueued event ──
        ev = JobEvent(
            job_id=job.id,
            event="enqueued",
            file_status="pending",
            source=source,
            detail=f"document_id={document_id}, file={file.filename}",
            timestamp=_utcnow(),
        )
        session.add(ev)
        session.commit()

        # ── Write file to queue/intake/ ──
        worker: Worker = request.app.state.worker
        try:
            await worker.enqueue_job_id_with_meta(
                job_id=job.id,
                file_content=content,
                filename=file.filename,
                source=source,
            )
        except Exception:
            logger.exception("Failed to write job to filesystem queue")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Processing queue is temporarily unavailable. Please retry later.",
            )

        return AcceptedResponse(document_id=document_id)

    @app.get(
        "/health",
        response_model=HealthResponse,
        summary="Service health check",
    )
    async def health(request: Request):
        """Verify service health including Tesseract and Ollama reachability."""
        settings: Settings = request.app.state.settings

        tesseract_ok = check_tesseract()
        ollama_ok = await check_ollama(settings)

        overall = "healthy" if (tesseract_ok and ollama_ok) else "degraded"

        resp = HealthResponse(
            status=overall,
            tesseract="ok" if tesseract_ok else "unreachable",
            ollama="ok" if ollama_ok else "unreachable",
        )

        status_code = 200 if overall == "healthy" else 503
        return JSONResponse(content=resp.model_dump(), status_code=status_code)

    return app


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
