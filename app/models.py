from datetime import datetime, timezone
from typing import Optional, List
import uuid
from sqlmodel import Field, SQLModel, Relationship


def _utcnow() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


class Job(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    status: str = Field(default="in_queue")  # in_queue, processing, completed, cancelled, failed
    context: Optional[str] = Field(default=None)
    webhook_url: Optional[str] = Field(default=None)
    legacy_document_id: Optional[int] = Field(default=None)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    files: List["JobFile"] = Relationship(back_populates="job", cascade_delete=True)


class JobFile(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.id")
    filename: str
    filepath: str
    status: str = Field(default="pending")  # pending, processing, completed, failed
    ocr_text: Optional[str] = Field(default=None)
    ai_result: Optional[str] = Field(default=None)
    error_message: Optional[str] = Field(default=None)

    job: Job = Relationship(back_populates="files")
