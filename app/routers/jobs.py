from fastapi import APIRouter, File, Form, UploadFile, HTTPException, Depends, Request
from typing import List, Optional
import uuid
import os
import shutil
from sqlmodel import Session, select
from app.database import get_session
from app.models import Job, JobFile
from app.schemas import JobResponse, JobFileResponse
from app.worker import Worker
from app.auth import verify_api_key

router = APIRouter(prefix="/api/v2/jobs", tags=["jobs"])

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

    # Create job
    job = Job(context=context, webhook_url=webhook_url)
    session.add(job)
    session.commit()
    session.refresh(job)

    job_dir = f"./data/jobs/{job.id}"
    os.makedirs(job_dir, exist_ok=True)

    for file in files:
        filepath = os.path.join(job_dir, file.filename)
        with open(filepath, "wb") as f:
            shutil.copyfileobj(file.file, f)
        
        job_file = JobFile(
            job_id=job.id,
            filename=file.filename,
            filepath=filepath
        )
        session.add(job_file)
    
    session.commit()
    session.refresh(job)

    # Enqueue job via filesystem queue
    worker: Worker = request.app.state.worker
    try:
        await worker.enqueue_job_id(job.id, source="http_v2")
    except Exception as e:
        raise HTTPException(status_code=503, detail="Processing queue is temporarily unavailable. Please retry later.")
    
    return job

@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: uuid.UUID, session: Session = Depends(get_session)):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@router.put("/{job_id}/cancel")
def cancel_job(job_id: uuid.UUID, session: Session = Depends(get_session)):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status in ["completed", "failed"]:
        raise HTTPException(status_code=400, detail="Cannot cancel a finished job")
    
    job.status = "cancelled"
    session.add(job)
    session.commit()
    return {"message": "Job cancelled"}

@router.delete("/{job_id}")
def delete_job(job_id: uuid.UUID, session: Session = Depends(get_session)):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_dir = f"./data/jobs/{job_id}"
    if os.path.exists(job_dir):
        shutil.rmtree(job_dir)
    
    session.delete(job)
    session.commit()
    return {"message": "Job deleted"}
