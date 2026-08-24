"""
End-to-end document processing pipeline.
"""

from __future__ import annotations

import logging
import uuid
import json
from typing import TYPE_CHECKING
from sqlmodel import Session

from app.database import engine
from app.llm import classify_document
from app.ocr import extract_text
from app.models import Job, JobFile
from app.schemas import ClassificationResult, WebhookPayload
from app.webhook import deliver_webhook

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
    """
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job or job.status == "cancelled":
            return
        
        job.status = "processing"
        session.add(job)
        session.commit()
        
        all_success = True
        
        for file in job.files:
            session.refresh(job) # check if cancelled
            if job.status == "cancelled":
                return
            
            file.status = "processing"
            session.add(file)
            session.commit()
            
            # OCR
            try:
                with open(file.filepath, "rb") as f:
                    file_content = f.read()
                ocr_text = extract_text(file_content, file.filename, settings)
                file.ocr_text = ocr_text
            except Exception as e:
                logger.exception("OCR failed")
                file.status = "failed"
                file.error_message = f"OCR failed: {str(e)}"
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
            except Exception as e:
                logger.exception("AI classification failed")
                file.status = "failed"
                file.error_message = f"AI classification failed: {str(e)}"
                all_success = False
            
            session.add(file)
            session.commit()
            
        # Finish job
        job.status = "completed" if all_success else "failed"
        session.add(job)
        session.commit()
        
        # Fire legacy webhook if needed
        if job.legacy_document_id:
            file = job.files[0]
            status_str = "success" if file.status == "completed" else "failed"
            classification = ClassificationResult()
            if file.ai_result:
                classification = ClassificationResult(**json.loads(file.ai_result))
                
            payload = WebhookPayload(
                document_id=job.legacy_document_id,
                status=status_str,
                ocr_text=file.ocr_text,
                classification=classification,
                error_message=file.error_message
            )
            await deliver_webhook(payload, settings)
