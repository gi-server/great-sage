"""
Great Sage — AI/Document Intelligence Service

FastAPI application providing:
  POST /api/v1/analyze  — Accept a document for async OCR + AI classification
  GET  /health          — Health check (Tesseract + Ollama reachability)
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse

from app.auth import verify_api_key
from app.config import Settings, load_settings
from app.llm import check_ollama
from app.ocr import check_tesseract, is_allowed_file
from app.schemas import AcceptedResponse, HealthResponse
from app.worker import Job, Worker

logger = logging.getLogger("great_sage")

# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle for the worker."""
    settings: Settings = app.state.settings
    worker = Worker(settings)
    app.state.worker = worker
    await worker.start()
    logger.info("Great Sage is ready")
    yield
    await worker.stop()
    logger.info("Great Sage shut down")


def create_app(settings: Settings | None = None) -> FastAPI:
    """
    Application factory.

    Accepts an optional Settings override for testing.
    """
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
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings

    # ── Routes ────────────────────────────────────────────────────────────

    @app.post(
        "/api/v1/analyze",
        response_model=AcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        summary="Submit a document for analysis",
    )
    async def analyze(
        request: Request,
        file: UploadFile = File(...),
        document_id: int = Form(...),
    ):
        """
        Accept a document file (PDF, PNG, JPG) for asynchronous processing.

        Returns HTTP 202 immediately. The result is delivered via webhook.
        """
        settings: Settings = request.app.state.settings

        # ── Auth ──
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

        # ── Enqueue for background processing ──
        worker: Worker = request.app.state.worker
        job = Job(
            document_id=document_id,
            file_content=content,
            filename=file.filename,
        )
        try:
            await worker.enqueue(job)
        except asyncio.QueueFull:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Processing queue is full. Please retry later.",
            )

        return AcceptedResponse(document_id=document_id)

    @app.get(
        "/health",
        response_model=HealthResponse,
        summary="Service health check",
    )
    async def health(request: Request):
        """
        Verify service health including Tesseract and Ollama reachability.
        """
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
# Entrypoint for `python -m app.main` or `uvicorn app.main:app`
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
