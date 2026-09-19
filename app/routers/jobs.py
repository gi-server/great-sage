from fastapi import APIRouter, File, Form, UploadFile, HTTPException, Depends, Request
from typing import List, Optional
import uuid
import os
from datetime import datetime, timezone
from sqlmodel import Session, select
from app.database import get_session, engine
from app.models import Job, JobFile, JobEvent
from app.schemas import JobResponse, JobFileResponse, JobDetailResponse, JobEventSchema
from app.worker import Worker
from app.auth import verify_api_key
from app import queue_manager
from pydantic import BaseModel

router = APIRouter(prefix="/api/v2/jobs", tags=["jobs"])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@router.post("", response_model=JobResponse, status_code=202)
async def create_job(
    request: Request,
    files: List[UploadFile] = File(...),
    context: Optional[str] = Form(None),
    webhook_url: Optional[str] = Form(None),
    session: Session = Depends(get_session)
):
    settings = request.app.state.settings
    if settings.great_sage_api_key:
        verify_api_key(
            settings,
            x_api_key=request.headers.get("X-API-Key"),
            authorization=request.headers.get("Authorization"),
        )

    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    source = request.headers.get("X-Source", "http_v2")

    # Create job
    job = Job(context=context, webhook_url=webhook_url, source=source, status="pending")
    session.add(job)
    session.commit()
    session.refresh(job)

    # Write enqueued event
    ev = JobEvent(
        job_id=job.id,
        event="enqueued",
        file_status="pending",
        source=source,
        detail=f"v2 batch: {len(files)} file(s)",
        timestamp=_utcnow(),
    )
    session.add(ev)

    worker: Worker = request.app.state.worker

    # For V2 batch, enqueue the first file only via queue (one job per upload)
    # If multiple files, write them all to intake/ under the same job_id
    for idx, file in enumerate(files):
        content = await file.read()
        intake_path = f"{settings.queue_dir}/intake/{job.id}/{file.filename}"
        job_file = JobFile(
            job_id=job.id,
            filename=file.filename,
            filepath=intake_path,
        )
        session.add(job_file)

        if idx == 0:
            # First file triggers the queue write — watcher picks it up
            try:
                await worker.enqueue_job_id_with_meta(
                    job_id=job.id,
                    file_content=content,
                    filename=file.filename,
                    source=source,
                )
            except Exception as e:
                raise HTTPException(
                    status_code=503,
                    detail="Processing queue is temporarily unavailable. Please retry later.",
                )
        else:
            # Additional files go directly into the intake/ directory (already created)
            import shutil
            from pathlib import Path
            dest = Path(settings.queue_dir) / "intake" / str(job.id) / file.filename
            dest.write_bytes(content)

    session.commit()
    session.refresh(job)
    return job


class JobQueryRequest(BaseModel):
    status: Optional[str] = None
    limit: int = 100
    offset: int = 0


@router.api_route("", methods=["GET", "QUERY"], response_model=List[JobResponse])
def list_jobs(
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    body: Optional[JobQueryRequest] = None,
    session: Session = Depends(get_session)
):
    query_status = body.status if body and body.status else status
    query_limit = body.limit if body and body.limit != 100 else limit
    query_offset = body.offset if body and body.offset != 0 else offset

    query = select(Job)
    if query_status:
        query = query.where(Job.status == query_status)

    query = query.offset(query_offset).limit(query_limit).order_by(Job.created_at.desc())
    jobs = session.exec(query).all()
    return jobs


@router.get("/{job_id}", response_model=JobDetailResponse)
def get_job(job_id: uuid.UUID, session: Session = Depends(get_session)):
    """
    Return full job detail including the live processing event log.

    Events are returned in chronological order and include file_status,
    raw_data (OCR snapshot), source, and timestamp at each stage.
    """
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    events = session.exec(
        select(JobEvent)
        .where(JobEvent.job_id == job_id)
        .order_by(JobEvent.timestamp)
    ).all()

    return JobDetailResponse(
        id=job.id,
        status=job.status,
        source=job.source,
        raw_data=job.raw_data,
        context=job.context,
        created_at=job.created_at,
        updated_at=job.updated_at,
        files=[
            JobFileResponse(
                id=f.id,
                filename=f.filename,
                status=f.status,
                ocr_text=f.ocr_text,
                ai_result=f.ai_result,
                error_message=f.error_message,
            )
            for f in job.files
        ],
        events=[
            JobEventSchema(
                event=e.event,
                file_status=e.file_status,
                source=e.source,
                raw_data=e.raw_data,
                detail=e.detail,
                timestamp=e.timestamp,
            )
            for e in events
        ],
    )


@router.put("/{job_id}/cancel")
def cancel_job(job_id: uuid.UUID, session: Session = Depends(get_session)):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in ["completed", "failed"]:
        raise HTTPException(status_code=400, detail="Cannot cancel a finished job")

    job.status = "cancelled"
    job.updated_at = _utcnow()
    session.add(job)
    session.commit()
    return {"message": "Job cancelled"}


@router.delete("/{job_id}")
def delete_job(
    job_id: uuid.UUID,
    request: Request,
    session: Session = Depends(get_session)
):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    settings = request.app.state.settings
    queue_manager.delete_job_dir(str(job_id), settings.queue_dir)

    session.delete(job)
    session.commit()
    return {"message": "Job deleted"}
