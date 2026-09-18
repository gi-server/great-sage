"""
End-to-end document processing pipeline.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from sqlmodel import Session, select

from app.database import engine
from app.llm import classify_document
from app.models import Job, JobFile, JobEvent
from app.ocr import extract_text
from app.schemas import ClassificationResult, FileResult, JobWebhookPayload, WebhookPayload
from app.webhook import deliver_job_webhook, deliver_webhook

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.pipeline")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _write_event(
    session: Session,
    job: Job,
    event: str,
    file_status: str,
    raw_data: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    """Append a JobEvent row and commit."""
    ev = JobEvent(
        job_id=job.id,
        event=event,
        file_status=file_status,
        source=job.source,
        raw_data=raw_data,
        detail=detail,
        timestamp=_utcnow(),
    )
    session.add(ev)
    session.commit()


async def process_document(
    document_id: int,
    file_content: bytes,
    filename: str,
    settings: "Settings",
) -> None:
    """
    Direct single-document processing pipeline (backward compatibility helper).

    Used by the legacy V1 path when a document is submitted without going
    through the DB-backed job system. Fires a webhook directly on completion.
    """
    ocr_text: str | None = None

    # Step 1: OCR
    try:
        ocr_text = extract_text(file_content, filename, settings)
    except Exception:
        logger.exception("OCR failed for document %d", document_id)
        payload = WebhookPayload(
            document_id=document_id,
            status="failed",
            ocr_text=None,
            classification=ClassificationResult(),
            error_message="OCR processing failed",
        )
        await deliver_webhook(payload, settings)
        return

    if not ocr_text:
        payload = WebhookPayload(
            document_id=document_id,
            status="failed",
            ocr_text="",
            classification=ClassificationResult(),
            error_message="OCR produced no text from the document",
        )
        await deliver_webhook(payload, settings)
        return

    # Step 2: AI Classification
    try:
        classification = await classify_document(ocr_text, settings)
    except Exception as exc:
        logger.exception("AI classification failed for document %d", document_id)
        payload = WebhookPayload(
            document_id=document_id,
            status="failed",
            ocr_text=ocr_text,
            classification=ClassificationResult(),
            error_message=f"AI classification failed: {exc}",
        )
        await deliver_webhook(payload, settings)
        return

    # Step 3: Success
    payload = WebhookPayload(
        document_id=document_id,
        status="success",
        ocr_text=ocr_text,
        classification=classification,
    )
    await deliver_webhook(payload, settings)


async def process_job(
    job_id: uuid.UUID,
    settings: "Settings",
) -> bool:
    """
    Process all files within a job.

    Files are read from their current location on disk — the worker leaves
    them in queue/intake/<job_id>/ throughout processing.  On success the
    worker (not this function) moves the folder to completed/.

    Returns (all_success, callback_ok) tuple, or just all_success for the
    legacy V1 path.
    """
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job or job.status == "cancelled":
            return False, True

        # Eagerly load all file records
        files = session.exec(select(JobFile).where(JobFile.job_id == job_id)).all()

        _write_event(session, job, "processing_started", "processing")

        all_success = True

        for file in files:
            # Check for cancellation
            current_status = session.exec(
                select(Job.status).where(Job.id == job_id)
            ).one()
            if current_status == "cancelled":
                return False, True

            file.status = "processing"

            # ── OCR ──────────────────────────────────────────────────────────
            try:
                file_path = Path(file.filepath)
                if not file_path.exists():
                    # Filepath may still reference old data/jobs path — try queue/intake
                    queue_intake_path = (
                        Path(settings.queue_dir) / "intake" / str(job_id) / file.filename
                    )
                    file_path = queue_intake_path

                with open(file_path, "rb") as f:
                    file_content = f.read()

                ocr_text = extract_text(file_content, file.filename, settings)
                file.ocr_text = ocr_text

            except Exception as exc:
                logger.exception("OCR failed for file %s in job %s", file.filename, job_id)
                file.status = "failed"
                file.error_message = f"OCR failed: {exc}"
                session.add(file)
                session.commit()
                all_success = False
                continue

            if not ocr_text:
                file.status = "failed"
                file.error_message = "OCR produced no text"
                session.add(file)
                session.commit()
                all_success = False
                continue

            # Store raw OCR text on the Job row (first file wins for multi-file jobs)
            if not job.raw_data:
                job.raw_data = ocr_text
                session.add(job)
                session.commit()

            _write_event(session, job, "ocr_done", "processing",
                         raw_data=ocr_text[:500] if ocr_text else None,
                         detail=f"File: {file.filename}")

            # ── AI Classification ─────────────────────────────────────────────
            try:
                classification = await classify_document(ocr_text, settings, context=job.context)
                file.ai_result = classification.model_dump_json()
                file.status = "completed"
                _write_event(session, job, "ai_done", "processing",
                             detail=f"File: {file.filename}, type: {classification.document_type}")
            except Exception as exc:
                logger.exception(
                    "AI classification failed for file %s in job %s", file.filename, job_id
                )
                file.status = "failed"
                file.error_message = f"AI classification failed: {exc}"
                all_success = False

            session.add(file)
            session.commit()

        # Note: job terminal status (completed/failed) is set by the worker,
        # not here, so the worker can handle the folder rename atomically.

        # ── V1 legacy webhook ─────────────────────────────────────────────────
        if job.legacy_document_id and files:
            first_file = files[0]
            status_str = "success" if first_file.status == "completed" else "failed"
            classification_result = ClassificationResult()
            if first_file.ai_result:
                classification_result = ClassificationResult(**json.loads(first_file.ai_result))

            payload = WebhookPayload(
                document_id=job.legacy_document_id,
                status=status_str,
                ocr_text=first_file.ocr_text,
                classification=classification_result,
                error_message=first_file.error_message,
            )
            await deliver_webhook(payload, settings)

        # ── V2 webhook ────────────────────────────────────────────────────────
        if job.webhook_url:
            file_results = []
            for file in files:
                file_classification = ClassificationResult()
                if file.ai_result:
                    file_classification = ClassificationResult(**json.loads(file.ai_result))

                file_results.append(
                    FileResult(
                        filename=file.filename,
                        status="success" if file.status == "completed" else "failed",
                        ocr_text=file.ocr_text,
                        classification=file_classification,
                        error_message=file.error_message,
                    )
                )

            v2_payload = JobWebhookPayload(
                job_id=str(job.id),
                status="success" if all_success else "failed",
                files=file_results,
            )
            callback_ok = await deliver_job_webhook(v2_payload, settings, custom_url=job.webhook_url)
            return all_success, callback_ok

    return all_success, True
