"""
Typed request / response models for the Great Sage API and internal pipeline.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# API request (metadata fields sent alongside the uploaded file)
# ---------------------------------------------------------------------------

class AnalyzeRequest(BaseModel):
    """Metadata expected as form fields on POST /api/v1/analyze."""
    document_id: int = Field(..., description="Poneglyph document ID for correlation")


# ---------------------------------------------------------------------------
# Accepted response (synchronous, returned immediately)
# ---------------------------------------------------------------------------

class AcceptedResponse(BaseModel):
    """Returned with HTTP 202 to confirm the job was enqueued."""
    message: str = "Document accepted for processing"
    document_id: int


# ---------------------------------------------------------------------------
# Classification result (nested inside webhook payload)
# ---------------------------------------------------------------------------

class ClassificationResult(BaseModel):
    """Structured AI extraction output."""
    document_type: Optional[str] = None
    person_name: Optional[str] = None
    dob: Optional[str] = None
    document_id_number: Optional[str] = None


# ---------------------------------------------------------------------------
# Webhook payload (pushed back to Poneglyph)
# ---------------------------------------------------------------------------

class WebhookPayload(BaseModel):
    """
    Unified result pushed to Poneglyph after processing.

    On success:
      status="success", classification populated, error_message=None
    On failure:
      status="failed", classification={}, error_message set.
      If OCR succeeded before the failure, ocr_text is still preserved.
    """
    document_id: int
    status: str = Field(..., pattern="^(success|failed)$")
    ocr_text: Optional[str] = None
    classification: ClassificationResult = Field(default_factory=ClassificationResult)
    error_message: Optional[str] = None


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    tesseract: str
    ollama: str

# ---------------------------------------------------------------------------
# V2 Job API Models
# ---------------------------------------------------------------------------

import uuid
from typing import List
from datetime import datetime

class JobFileResponse(BaseModel):
    id: uuid.UUID
    filename: str
    status: str
    ocr_text: Optional[str] = None
    ai_result: Optional[str] = None
    error_message: Optional[str] = None

class JobResponse(BaseModel):
    id: uuid.UUID
    status: str
    context: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    files: List[JobFileResponse] = []
