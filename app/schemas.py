"""
Typed request / response models for the Great Sage API and internal pipeline.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Accepted response (synchronous, returned immediately)
# ---------------------------------------------------------------------------

class AcceptedResponse(BaseModel):
    """Returned with HTTP 202 to confirm the job was enqueued."""
    message: str = "Document accepted for processing"
    document_id: int


# ---------------------------------------------------------------------------
# Document-type extraction schemas
# ---------------------------------------------------------------------------
# Each schema defines the CONTROLLED, PREDICTABLE fields that Great Sage
# extracts for a specific document type. The LLM output is validated against
# these schemas — arbitrary/unvalidated keys are rejected.

class IdentityExtraction(BaseModel):
    """Extraction schema for identity documents (passports, IDs, driver licenses)."""
    person_name: Optional[str] = None
    dob: Optional[str] = None
    document_id_number: Optional[str] = None


class InvoiceExtraction(BaseModel):
    """Extraction schema for invoices and bills."""
    invoice_number: Optional[str] = None
    vendor: Optional[str] = None
    date: Optional[str] = None
    due_date: Optional[str] = None
    gst: Optional[str] = None
    total: Optional[str] = None


class ReceiptExtraction(BaseModel):
    """Extraction schema for receipts."""
    merchant: Optional[str] = None
    purchase_date: Optional[str] = None
    items: Optional[str] = None
    total: Optional[str] = None


# Registry mapping document_type values to their extraction schema.
# Used by the pipeline to validate LLM output before sending to Poneglyph.
EXTRACTION_SCHEMAS: dict[str, type[BaseModel]] = {
    "identity_document": IdentityExtraction,
    "passport": IdentityExtraction,
    "drivers_license": IdentityExtraction,
    "national_id": IdentityExtraction,
    "invoice": InvoiceExtraction,
    "bill": InvoiceExtraction,
    "receipt": ReceiptExtraction,
}

# Fallback: identity extraction for unrecognized document types
DEFAULT_EXTRACTION_SCHEMA = IdentityExtraction


def validate_extraction(document_type: str | None, raw_data: dict[str, Any]) -> dict[str, Any]:
    """
    Validate raw LLM extraction output against the schema for the given document type.

    This ensures that:
    1. Only known, controlled field names are accepted (no arbitrary LLM keys).
    2. Values are coerced to the expected types.
    3. Unknown keys are silently dropped — not stored.

    Returns a clean dict suitable for JSONB storage in Poneglyph.
    """
    doc_type_key = (document_type or "").lower().strip().replace(" ", "_")
    schema_cls = EXTRACTION_SCHEMAS.get(doc_type_key, DEFAULT_EXTRACTION_SCHEMA)
    validated = schema_cls.model_validate(raw_data)
    # Only include non-None fields in the output
    return validated.model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# Classification result (nested inside webhook payload)
# ---------------------------------------------------------------------------

class ClassificationResult(BaseModel):
    """
    Structured AI extraction output.

    extracted_data is the canonical field containing validated, document-type-specific
    key-value pairs. Legacy fields (person_name, dob, document_id_number) are populated
    for backward compatibility with older Poneglyph versions.
    """
    document_type: Optional[str] = None
    extracted_data: dict[str, Any] = Field(default_factory=dict)
    # Legacy fields — kept for backward compatibility with Poneglyph webhook consumers.
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


class FileResult(BaseModel):
    filename: str
    status: str = Field(..., pattern="^(success|failed)$")
    ocr_text: Optional[str] = None
    classification: ClassificationResult = Field(default_factory=ClassificationResult)
    error_message: Optional[str] = None


class JobWebhookPayload(BaseModel):
    """Payload pushed to Poneglyph when a multi-file Job completes."""
    job_id: str
    status: str = Field(..., pattern="^(success|failed)$")
    files: list[FileResult]


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    tesseract: str
    ollama: str


# ---------------------------------------------------------------------------
# V2 Job API response models
# ---------------------------------------------------------------------------

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
