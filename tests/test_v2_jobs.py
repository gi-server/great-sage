import io
import uuid
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock
from app.main import create_app
from app.config import Settings
from app.schemas import ClassificationResult

@pytest.fixture
def client():
    settings = Settings(
        great_sage_api_key="test-secret-key",
        max_upload_bytes=10 * 1024 * 1024,
        poneglyph_webhook_secret="test-webhook-secret",
        poneglyph_webhook_url="http://127.0.0.1:8080/api/internal/webhook/analyze"
    )
    app = create_app(settings)
    with TestClient(app) as test_client:
        yield test_client

def test_v2_job_lifecycle(client):
    headers = {"X-API-Key": "test-secret-key"}
    
    # 1. POST /api/v2/jobs (Multi-file upload)
    files = [
        ("files", ("invoice.pdf", io.BytesIO(b"%PDF-1.4 dummy pdf content"), "application/pdf")),
        ("files", ("receipt.png", io.BytesIO(b"\x89PNG\r\n\x1a\n dummy png content"), "image/png"))
    ]
    data = {"context": "Test batch onboarding verification"}
    
    response = client.post("/api/v2/jobs", files=files, data=data, headers=headers)
    assert response.status_code == 202
    job_data = response.json()
    assert "id" in job_data
    assert job_data["status"] == "in_queue"
    assert job_data["context"] == "Test batch onboarding verification"
    assert len(job_data["files"]) == 2
    
    job_id = job_data["id"]

    # 2. GET /api/v2/jobs/{job_id}
    get_res = client.get(f"/api/v2/jobs/{job_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == job_id
    assert len(get_res.json()["files"]) == 2

    # 3. DELETE /api/v2/jobs/{job_id}
    del_res = client.delete(f"/api/v2/jobs/{job_id}")
    assert del_res.status_code == 200
    assert del_res.json()["message"] == "Job deleted"

    # Verify 404 after deletion
    get_res3 = client.get(f"/api/v2/jobs/{job_id}")
    assert get_res3.status_code == 404

def test_v2_job_cancellation(client):
    from app.database import engine
    from app.models import Job
    from sqlmodel import Session
    
    # Create a job directly in "in_queue" state to test cancel endpoint
    job_id = uuid.uuid4()
    with Session(engine) as session:
        job = Job(id=job_id, status="in_queue", context="Batch test")
        session.add(job)
        session.commit()

    # Cancel the queued job
    cancel_res = client.put(f"/api/v2/jobs/{job_id}/cancel")
    assert cancel_res.status_code == 200
    assert cancel_res.json()["message"] == "Job cancelled"

    # Verify status changed to cancelled
    get_res = client.get(f"/api/v2/jobs/{job_id}")
    assert get_res.status_code == 200
    assert get_res.json()["status"] == "cancelled"

def test_legacy_v1_analyze_endpoint(client):
    headers = {"X-API-Key": "test-secret-key"}
    file_payload = {"file": ("document.pdf", io.BytesIO(b"%PDF-1.4 dummy"), "application/pdf")}
    form_data = {"document_id": 99}

    response = client.post("/api/v1/analyze", files=file_payload, data=form_data, headers=headers)
    assert response.status_code == 202
    data = response.json()
    assert data["document_id"] == 99
    assert data["message"] == "Document accepted for processing"

@pytest.mark.asyncio
async def test_pipeline_execution_success():
    from app.pipeline import process_job
    from app.database import engine, init_db
    from app.models import Job, JobFile
    from sqlmodel import Session
    import os

    init_db()
    
    # Setup dummy job and file
    job_id = uuid.uuid4()
    job_dir = f"./data/jobs/{job_id}"
    os.makedirs(job_dir, exist_ok=True)
    dummy_filepath = os.path.join(job_dir, "test.pdf")
    with open(dummy_filepath, "wb") as f:
        f.write(b"Dummy content")

    with Session(engine) as session:
        job = Job(id=job_id, context="Check tax ID")
        session.add(job)
        file_rec = JobFile(job_id=job_id, filename="test.pdf", filepath=dummy_filepath)
        session.add(file_rec)
        session.commit()

    settings = Settings(
        great_sage_api_key="test-key",
        max_upload_bytes=10*1024*1024,
        poneglyph_webhook_secret="test-secret"
    )

    mock_classification = ClassificationResult(
        document_type="Invoice",
        person_name="John Smith",
        dob=None,
        document_id_number="INV-12345"
    )

    with patch("app.pipeline.extract_text", return_value="Invoice INV-12345 for John Smith"):
        with patch("app.pipeline.classify_document", new_callable=AsyncMock, return_value=mock_classification):
            await process_job(job_id, settings)

    # Check updated database record
    with Session(engine) as session:
        updated_job = session.get(Job, job_id)
        assert updated_job.status == "completed"
        assert updated_job.files[0].status == "completed"
        assert updated_job.files[0].ocr_text == "Invoice INV-12345 for John Smith"
        assert "INV-12345" in updated_job.files[0].ai_result
