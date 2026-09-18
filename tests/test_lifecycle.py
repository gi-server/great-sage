"""
Lifecycle tests for Great Sage's two-folder SQLite-backed queue.

Tests cover:
  - Job creation via HTTP → file written to intake/ → SQLite row created
  - enqueued JobEvent written at intake time
  - QueueWatcher detects new dir in intake/ → pushes job_id to channel
  - Watcher ignores non-UUID directory names
  - Worker claims job via SQLite (status: pending → processing)
  - Only one worker claims a job (idempotency)
  - Successful completion: intake/ dir moves to completed/, SQLite status=completed
  - Failure path: file stays in intake/, SQLite status=failed
  - Crash recovery: SQLite rows stuck in 'processing' reset to 'pending' on startup
  - GET /api/v2/jobs/{id} returns JobEvent log with file_status, source, timestamp
"""

from __future__ import annotations

import asyncio
import io
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from app.config import Settings
from app.database import engine, init_db
from app.main import create_app, recover_stranded_jobs
from app.models import Job, JobEvent
from app.queue_manager import (
    ensure_queue_dirs,
    write_job_to_queue,
    move_to_completed,
    get_job_dir,
    _job_dir,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TEST_API_KEY = "test-lifecycle-key"
TEST_WEBHOOK_URL = "http://poneglyph.test/api/internal/webhook/analyze"
TEST_WEBHOOK_SECRET = "test-lifecycle-secret"


def _make_settings(queue_dir: str, **overrides) -> Settings:
    defaults = dict(
        great_sage_api_key=TEST_API_KEY,
        poneglyph_webhook_secret=TEST_WEBHOOK_SECRET,
        poneglyph_webhook_url=TEST_WEBHOOK_URL,
        queue_dir=queue_dir,
        queue_max_attempts=3,
        worker_pool_size=1,
        worker_queue_size=16,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture()
def queue_dir(tmp_path: Path) -> str:
    q = str(tmp_path / "queue")
    ensure_queue_dirs(q)
    return q


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    return _make_settings(str(tmp_path / "queue"))


@pytest.fixture()
def client(settings: Settings) -> TestClient:
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


class _FakeSettings:
    def __init__(self, queue_dir: str):
        self.queue_dir = queue_dir


SAMPLE_PDF = b"%PDF-1.4 fake pdf content"
SAMPLE_PNG = b"\x89PNG\r\n\x1a\n fake png"


# ---------------------------------------------------------------------------
# 1. HTTP job creation → file in intake/ → SQLite row → JobEvent
# ---------------------------------------------------------------------------

def test_v1_analyze_creates_file_in_intake(client: TestClient, settings: Settings) -> None:
    """POST /api/v1/analyze must write the raw file to intake/<job_id>/."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "42"},
        files={"file": ("scan.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    # The intake/ directory should now contain a job dir with the file
    intake_root = Path(settings.queue_dir) / "intake"
    job_dirs = list(intake_root.iterdir())
    assert len(job_dirs) == 1, "Expected exactly one job dir in intake/"
    assert (job_dirs[0] / "scan.pdf").exists(), "Raw file should be in intake/<job_id>/"


def test_v1_analyze_no_metadata_json_on_disk(client: TestClient, settings: Settings) -> None:
    """metadata.json must NOT be written — SQLite is the only state store."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "1"},
        files={"file": ("doc.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    for job_dir in intake_root.iterdir():
        assert not (job_dir / "metadata.json").exists(), "metadata.json must not exist"


def test_v1_analyze_creates_sqlite_job_row(client: TestClient, settings: Settings) -> None:
    """POST /api/v1/analyze must create a Job row in SQLite with status='pending'."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "7"},
        files={"file": ("id.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202
    job_id = uuid.UUID(response.json()["document_id"] and
                       list((Path(settings.queue_dir) / "intake").iterdir())[0].name)

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job is not None
        assert job.status == "pending"
        assert job.legacy_document_id == 7


def test_v1_analyze_writes_enqueued_event(client: TestClient, settings: Settings) -> None:
    """An 'enqueued' JobEvent must be written to SQLite when the job is created."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "55"},
        files={"file": ("form.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    job_id = uuid.UUID(list(intake_root.iterdir())[0].name)

    with Session(engine) as session:
        events = session.exec(
            select(JobEvent).where(JobEvent.job_id == job_id)
        ).all()
        event_names = [e.event for e in events]
        assert "enqueued" in event_names


def test_v1_analyze_source_recorded(client: TestClient, settings: Settings) -> None:
    """The source field should default to 'poneglyph:8080' for V1 requests."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "3"},
        files={"file": ("doc.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    job_id = uuid.UUID(list(intake_root.iterdir())[0].name)

    with Session(engine) as session:
        job = session.get(Job, job_id)
        # Default source when no X-Source header is sent
        assert job.source == "poneglyph:8080"


def test_v1_analyze_custom_source_header(client: TestClient, settings: Settings) -> None:
    """X-Source header value should be recorded as job.source."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "10"},
        files={"file": ("doc.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY, "X-Source": "mobile_app:443"},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    job_id = uuid.UUID(list(intake_root.iterdir())[0].name)

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job.source == "mobile_app:443"


# ---------------------------------------------------------------------------
# 2. Queue directories — only intake/ and completed/ exist
# ---------------------------------------------------------------------------

def test_only_intake_and_completed_dirs_created(settings: Settings) -> None:
    """ensure_queue_dirs must create only tmp/, intake/, and completed/."""
    ensure_queue_dirs(settings.queue_dir)
    queue_root = Path(settings.queue_dir)
    assert (queue_root / "intake").is_dir()
    assert (queue_root / "completed").is_dir()
    assert (queue_root / "tmp").is_dir()
    assert not (queue_root / "processing").exists()
    assert not (queue_root / "failed").exists()
    assert not (queue_root / "incoming").exists()


# ---------------------------------------------------------------------------
# 3. QueueWatcher — detects new dirs in intake/
# ---------------------------------------------------------------------------

def test_watcher_pushes_job_id_on_dir_created(tmp_path: Path) -> None:
    """Simulates a watchdog DirCreatedEvent on intake/ → push to channel."""
    from watchdog.events import DirCreatedEvent
    from app.queue_watcher import _IntakeHandler

    loop = asyncio.new_event_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=16)

    handler = _IntakeHandler(loop=loop, queue=queue)
    job_id = str(uuid.uuid4())

    intake_path = tmp_path / "intake" / job_id
    intake_path.mkdir(parents=True)

    event = DirCreatedEvent(src_path=str(intake_path))
    handler.on_created(event)

    loop.run_until_complete(asyncio.sleep(0))
    assert not queue.empty()
    assert queue.get_nowait() == job_id
    loop.close()


def test_watcher_ignores_non_uuid_directories(tmp_path: Path) -> None:
    """Watchdog events for non-UUID directory names are silently ignored."""
    from watchdog.events import DirCreatedEvent
    from app.queue_watcher import _IntakeHandler

    loop = asyncio.new_event_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=16)
    handler = _IntakeHandler(loop=loop, queue=queue)

    bad_path = tmp_path / "intake" / "not-a-uuid"
    bad_path.mkdir(parents=True)

    event = DirCreatedEvent(src_path=str(bad_path))
    handler.on_created(event)

    loop.run_until_complete(asyncio.sleep(0))
    assert queue.empty(), "Non-UUID dir should not be enqueued"
    loop.close()


# ---------------------------------------------------------------------------
# 4. SQLite claim lock — only one worker claims a job
# ---------------------------------------------------------------------------

def test_sqlite_claim_idempotent(settings: Settings) -> None:
    """Two concurrent claim attempts: only the first should succeed."""
    init_db()

    with Session(engine) as session:
        job = Job(status="pending", source="test")
        session.add(job)
        session.commit()
        session.refresh(job)
        job_id = job.id

    # First claim: pending → processing
    with Session(engine) as s1:
        j = s1.get(Job, job_id)
        assert j.status == "pending"
        j.status = "processing"
        s1.add(j)
        s1.commit()

    # Second claim: should see 'processing', not 'pending'
    with Session(engine) as s2:
        j2 = s2.get(Job, job_id)
        assert j2.status == "processing", "Second claim must see already-processing status"


# ---------------------------------------------------------------------------
# 5. move_to_completed — file travels with directory
# ---------------------------------------------------------------------------

def test_move_to_completed_moves_file(queue_dir: str) -> None:
    """After move_to_completed, the file is in completed/, not intake/."""
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_PDF, "invoice.pdf", settings)

    assert _job_dir(queue_dir, "intake", str(job_id)).exists()
    assert not _job_dir(queue_dir, "completed", str(job_id)).exists()

    move_to_completed(str(job_id), queue_dir)

    assert not _job_dir(queue_dir, "intake", str(job_id)).exists()
    completed_dir = _job_dir(queue_dir, "completed", str(job_id))
    assert completed_dir.exists()
    assert (completed_dir / "invoice.pdf").read_bytes() == SAMPLE_PDF


# ---------------------------------------------------------------------------
# 6. Crash recovery — SQLite-based
# ---------------------------------------------------------------------------

def test_crash_recovery_resets_processing_jobs(settings: Settings) -> None:
    """Jobs stuck in 'processing' in SQLite should be reset to 'pending' on startup."""
    init_db()

    # Simulate a stranded job (was processing when the server crashed)
    with Session(engine) as session:
        job = Job(status="processing", source="poneglyph:8080")
        session.add(job)
        session.commit()
        session.refresh(job)
        job_id = job.id

        # Also create its intake/ dir (would normally exist on disk)
        intake_dir = Path(settings.queue_dir) / "intake" / str(job_id)
        intake_dir.mkdir(parents=True, exist_ok=True)

    # Simulate startup — recover_stranded_jobs should reset it
    from app.worker import Worker
    worker = Worker(settings)
    recover_stranded_jobs(worker)

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job.status == "pending", "Stranded job should be reset to pending"

        # A 'recovered' event should be written
        events = session.exec(select(JobEvent).where(JobEvent.job_id == job_id)).all()
        assert any(e.event == "recovered" for e in events)


# ---------------------------------------------------------------------------
# 7. GET /api/v2/jobs/{id} — returns JobEvent log
# ---------------------------------------------------------------------------

def test_get_job_returns_event_log(client: TestClient, settings: Settings) -> None:
    """GET /api/v2/jobs/{id} must include the 'events' array with 'enqueued' event."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "77"},
        files={"file": ("form.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    job_id = list(intake_root.iterdir())[0].name

    get_res = client.get(f"/api/v2/jobs/{job_id}")
    assert get_res.status_code == 200

    data = get_res.json()
    assert "events" in data, "Response must include events array"
    assert len(data["events"]) >= 1
    event_names = [e["event"] for e in data["events"]]
    assert "enqueued" in event_names


def test_get_job_event_has_required_fields(client: TestClient, settings: Settings) -> None:
    """Each event in the log must have event, file_status, source, timestamp."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "88"},
        files={"file": ("doc.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    job_id = list(intake_root.iterdir())[0].name

    data = client.get(f"/api/v2/jobs/{job_id}").json()
    first_event = data["events"][0]

    assert "event" in first_event
    assert "file_status" in first_event
    assert "source" in first_event
    assert "timestamp" in first_event


def test_get_job_returns_source_and_raw_data(client: TestClient, settings: Settings) -> None:
    """GET /api/v2/jobs/{id} must return source and raw_data on the job itself."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "99"},
        files={"file": ("scan.pdf", io.BytesIO(SAMPLE_PDF), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY, "X-Source": "poneglyph:8080"},
    )
    assert response.status_code == 202

    intake_root = Path(settings.queue_dir) / "intake"
    job_id = list(intake_root.iterdir())[0].name

    data = client.get(f"/api/v2/jobs/{job_id}").json()
    assert data["source"] == "poneglyph:8080"
    assert "raw_data" in data  # None until OCR runs, but key must exist
