from datetime import datetime, timezone
from typing import Optional, List
import uuid
from sqlmodel import Field, SQLModel, Relationship


def _utcnow() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


class Job(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    status: str = Field(default="pending")  # pending, processing, completed, failed, cancelled
    source: str = Field(default="unknown")  # e.g. "poneglyph:8080", "http_v2"
    raw_data: Optional[str] = Field(default=None)  # OCR text, populated after OCR stage
    context: Optional[str] = Field(default=None)
    webhook_url: Optional[str] = Field(default=None)
    legacy_document_id: Optional[int] = Field(default=None)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    files: List["JobFile"] = Relationship(back_populates="job", cascade_delete=True)
    events: List["JobEvent"] = Relationship(back_populates="job", cascade_delete=True)


class JobFile(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.id", index=True)
    filename: str
    filepath: str           # path on disk — inside queue/intake/<job_id>/ or queue/completed/<job_id>/
    status: str = Field(default="pending")  # pending, processing, completed, failed
    ocr_text: Optional[str] = Field(default=None)
    ai_result: Optional[str] = Field(default=None)
    error_message: Optional[str] = Field(default=None)

    job: Job = Relationship(back_populates="files")


class JobEvent(SQLModel, table=True):
    """
    Append-only processing log for a job.

    Written at every significant stage transition so the GET /jobs/{id}
    API can return a live, structured audit trail without touching the
    filesystem at all.
    """
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.id", index=True)
    event: str              # enqueued | claimed | processing_started | ocr_done | ai_done | completed | retried | failed
    file_status: str        # mirrors Job.status at the time this event was written
    source: str             # originating application, e.g. "poneglyph:8080"
    raw_data: Optional[str] = Field(default=None)   # OCR snapshot at this point (None until OCR runs)
    detail: Optional[str] = Field(default=None)     # human-readable context / error message
    timestamp: datetime = Field(default_factory=_utcnow)

    job: Job = Relationship(back_populates="events")
