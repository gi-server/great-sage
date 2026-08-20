"""
End-to-end document processing pipeline.

Orchestrates:  file → OCR → text extraction → AI classification → webhook result

Graceful degradation:
  - If OCR fails → webhook with status=failed, empty classification.
  - If OCR succeeds but LLM fails → webhook preserves ocr_text, empty classification, error_message set.
  - If everything succeeds → webhook with status=success, full classification.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.llm import classify_document
from app.ocr import extract_text
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
    Run the full document processing pipeline and deliver the result via webhook.

    This function is designed to be called from the background worker — it
    never raises. All outcomes are communicated through the webhook payload.
    """
    ocr_text: str | None = None

    # ── Step 1: OCR ──────────────────────────────────────────────────────
    try:
        ocr_text = extract_text(file_content, filename, settings)
        logger.info("OCR succeeded for document %d (%d chars)", document_id, len(ocr_text))
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

    # ── Step 2: AI Classification ────────────────────────────────────────
    if not ocr_text:
        # OCR succeeded but produced no text — still report cleanly
        payload = WebhookPayload(
            document_id=document_id,
            status="failed",
            ocr_text="",
            classification=ClassificationResult(),
            error_message="OCR produced no text from the document",
        )
        await deliver_webhook(payload, settings)
        return

    try:
        classification = await classify_document(ocr_text, settings)
        logger.info("AI classification succeeded for document %d", document_id)
    except Exception as exc:
        # ── Graceful degradation: OCR succeeded, LLM failed ──────────
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

    # ── Step 3: Success ──────────────────────────────────────────────────
    payload = WebhookPayload(
        document_id=document_id,
        status="success",
        ocr_text=ocr_text,
        classification=classification,
    )
    await deliver_webhook(payload, settings)
