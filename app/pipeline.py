"""
End-to-end document processing pipeline.
"""

from __future__ import annotations

import logging
import uuid
import json
from typing import TYPE_CHECKING
from sqlmodel import Session, select

from app.database import engine
from app.llm import classify_document
from app.ocr import extract_text
from app.models import Job, JobFile
from app.schemas import ClassificationResult, WebhookPayload, JobWebhookPayload, FileResult
from app.webhook import deliver_webhook, deliver_job_webhook
from app import trie_store
from app.queue_manager import JobMeta

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.pipeline")


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
) -> None:
    """
    Process all files within a job.

    Reads the job and all its files in a single query at the start.
    Batches all per-file DB writes into a single commit at the end of each
    file, rather than committing after every individual field update.
    Cancellation is checked once per file iteration without an extra
    session.refresh() round-trip inside the loop.
    """
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job or job.status == "cancelled":
            return

        # Eagerly load all file records so we don't re-query inside the loop
        files = session.exec(select(JobFile).where(JobFile.job_id == job_id)).all()

        job.status = "processing"
        session.add(job)
        session.commit()

        all_success = True

        for file in files:
            # Re-fetch just the job status to check for cancellation.
            # This is a single-column read — significantly cheaper than refresh().
            current_status = session.exec(
                select(Job.status).where(Job.id == job_id)
            ).one()
            if current_status == "cancelled":
                return

            file.status = "processing"

            # OCR
            try:
                with open(file.filepath, "rb") as f:
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

            # AI Classification
            try:
                classification = await classify_document(ocr_text, settings, context=job.context)
                file.ai_result = classification.model_dump_json()
                file.status = "completed"
            except Exception as exc:
                logger.exception("AI classification failed for file %s in job %s", file.filename, job_id)
                file.status = "failed"
                file.error_message = f"AI classification failed: {exc}"
                all_success = False

            session.add(file)
            session.commit()

        # Mark job terminal state
        job.status = "completed" if all_success else "failed"
        session.add(job)
        session.commit()

        # Fire V1 legacy webhook if this was submitted via /api/v1/analyze
        if job.legacy_document_id and files:
            first_file = files[0]
            status_str = "success" if first_file.status == "completed" else "failed"
            classification = ClassificationResult()
            if first_file.ai_result:
                classification = ClassificationResult(**json.loads(first_file.ai_result))

            payload = WebhookPayload(
                document_id=job.legacy_document_id,
                status=status_str,
                ocr_text=first_file.ocr_text,
                classification=classification,
                error_message=first_file.error_message,
            )
            await deliver_webhook(payload, settings)

        # Fire V2 webhook if a webhook_url was provided by the caller
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
                status="success" if job.status == "completed" else "failed",
                files=file_results,
            )
            await deliver_job_webhook(v2_payload, settings)


async def process_queue_job(
    job_id: uuid.UUID,
    meta: "JobMeta | None",
    settings: "Settings",
) -> None:
    """
    Wrapper around process_job() that additionally persists results to the
    person-centric trie store.

    This is called by the filesystem-backed worker pool.  The existing
    process_job() function is invoked unchanged; after it completes, we
    read the AI results back out of SQLite and write them to
    data/people/<person_id>/trie/.

    Person resolution
    -----------------
    If the job metadata already carries a person_id (supplied by Poneglyph at
    submission time), that is used directly.  Otherwise we use the LLM-extracted
    person_name + dob to look up or create a person via trie_store.

    Idempotency
    -----------
    trie_store.insert() checks whether this job_id has already been written
    for the person before doing any work, so retries are safe.
    """
    # Run the core OCR / LLM / webhook pipeline (unchanged)
    await process_job(job_id=job_id, settings=settings)

    # Read back the results to persist into the trie
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job:
            logger.warning("process_queue_job: job %s not found in DB after processing", job_id)
            return

        files = session.exec(select(JobFile).where(JobFile.job_id == job_id)).all()

    job_id_str = str(job_id)

    for file in files:
        if file.status != "completed" or not file.ai_result:
            continue

        try:
            ai = json.loads(file.ai_result)
        except Exception:
            logger.warning("process_queue_job: could not parse ai_result for file %s", file.id)
            continue

        person_name: str | None = ai.get("person_name") or (
            ai.get("extracted_data", {}).get("person_name") if isinstance(ai.get("extracted_data"), dict) else None
        )
        dob: str | None = ai.get("dob") or (
            ai.get("extracted_data", {}).get("dob") if isinstance(ai.get("extracted_data"), dict) else None
        )

        if not person_name:
            logger.debug(
                "process_queue_job: no person_name extracted for job %s file %s — skipping trie write",
                job_id_str, file.filename,
            )
            continue

        # Resolve person_id
        if meta and meta.person_id:
            person_id = meta.person_id
        else:
            person_id = trie_store.get_or_create_person(person_name, dob)

        # Persist to trie (idempotent)
        trie_store.insert(
            person_id=person_id,
            person_name=person_name,
            extracted_data=ai,
            job_id=job_id_str,
            dob=dob,
        )

    logger.info("process_queue_job: trie update complete for job %s", job_id_str)
