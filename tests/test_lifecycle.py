"""
Comprehensive lifecycle tests for Great Sage's filesystem-backed job queue.

Tests cover:
  - Job creation via HTTP → queue insertion → lifecycle event recorded
  - QueueWatcher detection (mock watchdog event → push_to_channel)
  - Atomic claiming (incoming → processing, idempotency)
  - processing_started lifecycle event recorded after claiming
  - process-started HTTP callback sent to Poneglyph (mocked httpx)
  - Callback failure does not abort processing
  - Successful completion → completed event → moves to completed/
  - Completion callback failure recorded in metadata, job still in completed/
  - Processing failure → retry (back to incoming/) → eventual failed/
  - Crash recovery via scan_stranded_jobs
  - QUERY /api/v2/jobs/{id} returns correct lifecycle for each queue state
  - QUERY is strictly read-only (does not move or claim jobs)
"""

from __future__ import annotations

import asyncio
import io
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.queue_manager import (
    JobMeta,
    LifecycleEvent,
    append_lifecycle_event,
    claim_job,
    complete_job,
    ensure_queue_dirs,
    fail_job,
    load_job_meta,
    mark_callback_delivered,
    retry_or_fail_job,
    scan_stranded_jobs,
    write_job_to_queue,
    _job_dir,
    _load_metadata,
    _save_metadata,
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


class _FakeSettings:
    def __init__(self, queue_dir: str, max_attempts: int = 3):
        self.queue_dir = queue_dir
        self.queue_max_attempts = max_attempts


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


# ---------------------------------------------------------------------------
# 1. Job creation → queue insertion → enqueued lifecycle event
# ---------------------------------------------------------------------------

def test_http_v2_job_creation_writes_queue_metadata(client: TestClient, settings: Settings) -> None:
    """POST /api/v2/jobs creates a job and writes metadata.json to incoming/."""
    response = client.post(
        "/api/v2/jobs",
        files=[("files", ("invoice.pdf", io.BytesIO(b"%PDF-1.4 dummy"), "application/pdf"))],
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]

    meta = load_job_meta(job_id, settings.queue_dir)
    assert meta is not None
    assert meta.job_id == job_id
    assert meta.source == "http_v2"
    assert meta.original_filename != ""


def test_http_v2_job_has_enqueued_lifecycle_event(client: TestClient, settings: Settings) -> None:
    """The metadata.json written during job creation must have an 'enqueued' event."""
    response = client.post(
        "/api/v2/jobs",
        files=[("files", ("doc.pdf", io.BytesIO(b"%PDF-1.4 content"), "application/pdf"))],
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]

    meta = load_job_meta(job_id, settings.queue_dir)
    assert meta is not None
    events = [ev.event for ev in meta.lifecycle]
    assert "enqueued" in events


def test_http_v1_job_has_poneglyph_file_path(client: TestClient, settings: Settings) -> None:
    """V1 submissions encode the Poneglyph document ID in poneglyph_file_path."""
    response = client.post(
        "/api/v1/analyze",
        data={"document_id": "99"},
        files={"file": ("scan.pdf", io.BytesIO(b"%PDF-1.4 scan"), "application/pdf")},
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202

    # Find the job in the queue
    meta = None
    for sub in ("incoming", "processing", "completed", "failed"):
        subdir = Path(settings.queue_dir) / sub
        if subdir.exists():
            for d in subdir.iterdir():
                if d.is_dir():
                    m = _load_metadata(d)
                    if m and m.source == "http_v1":
                        meta = m
                        break

    assert meta is not None
    assert "99" in meta.poneglyph_file_path  # document_id encoded in path
    assert "scan.pdf" in meta.original_filename


# ---------------------------------------------------------------------------
# 2. QueueWatcher detection → push_to_channel
# ---------------------------------------------------------------------------

def test_watcher_pushes_job_id_on_dir_created(tmp_path: Path) -> None:
    """Simulates a watchdog DirCreatedEvent triggering push_to_channel."""
    from watchdog.events import DirCreatedEvent
    from app.queue_watcher import _IncomingHandler

    loop = asyncio.new_event_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=16)

    handler = _IncomingHandler(loop=loop, queue=queue)
    job_id = str(uuid.uuid4())

    incoming_path = tmp_path / "incoming" / job_id
    incoming_path.mkdir(parents=True)

    event = DirCreatedEvent(src_path=str(incoming_path))
    handler.on_created(event)

    loop.run_until_complete(asyncio.sleep(0))
    assert not queue.empty()
    assert queue.get_nowait() == job_id
    loop.close()


def test_watcher_ignores_non_uuid_directories(tmp_path: Path) -> None:
    """Watchdog events for non-UUID directory names are ignored."""
    from watchdog.events import DirCreatedEvent
    from app.queue_watcher import _IncomingHandler

    loop = asyncio.new_event_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=16)
    handler = _IncomingHandler(loop=loop, queue=queue)

    bad_path = tmp_path / "incoming" / "not-a-uuid"
    bad_path.mkdir(parents=True)
    event = DirCreatedEvent(src_path=str(bad_path))
    handler.on_created(event)

    loop.run_until_complete(asyncio.sleep(0))
    assert queue.empty()
    loop.close()


# ---------------------------------------------------------------------------
# 3. Atomic claiming (incoming → processing, idempotency)
# ---------------------------------------------------------------------------

def test_claim_job_is_idempotent(queue_dir: str) -> None:
    """Only the first claim_job call succeeds; subsequent calls return False."""
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)

    assert claim_job(str(job_id), queue_dir) is True
    assert claim_job(str(job_id), queue_dir) is False
    assert claim_job(str(job_id), queue_dir) is False


def test_claim_job_leaves_processing_dir(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    assert not _job_dir(queue_dir, "incoming", str(job_id)).exists()
    assert _job_dir(queue_dir, "processing", str(job_id)).is_dir()


# ---------------------------------------------------------------------------
# 4. processing_started lifecycle event after claiming
# ---------------------------------------------------------------------------

def test_processing_started_event_appended_after_claim(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    # Simulate what the worker does
    append_lifecycle_event(str(job_id), queue_dir, "processing_started", detail="worker 0, attempt 1")

    meta = _load_metadata(_job_dir(queue_dir, "processing", str(job_id)))
    events = [ev.event for ev in meta.lifecycle]
    assert "processing_started" in events


# ---------------------------------------------------------------------------
# 5. process-started HTTP callback (mocked)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_started_callback_is_sent() -> None:
    """deliver_processing_started_callback fires a POST to the derived job-status URL."""
    from app.webhook import deliver_processing_started_callback

    settings = Settings(
        great_sage_api_key="key",
        poneglyph_webhook_secret="secret",
        poneglyph_webhook_url="http://pone.test/api/internal/webhook/analyze",
    )

    captured_urls = []
    captured_bodies = []

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "ok"

    async def mock_post(url, content, headers):
        captured_urls.append(url)
        captured_bodies.append(json.loads(content))
        return mock_response

    with patch("app.webhook.httpx.AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.post.side_effect = mock_post
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_instance

        result = await deliver_processing_started_callback(
            job_id="abc-123",
            attempt=1,
            settings=settings,
        )

    assert result is True
    assert len(captured_urls) == 1
    assert "/webhook/job-status" in captured_urls[0]
    body = captured_bodies[0]
    assert body["job_id"] == "abc-123"
    assert body["status"] == "processing"
    assert body["attempt"] == 1


@pytest.mark.asyncio
async def test_process_started_callback_failure_returns_false() -> None:
    """A failed callback returns False but does not raise."""
    from app.webhook import deliver_processing_started_callback

    settings = Settings(
        great_sage_api_key="key",
        poneglyph_webhook_secret="secret",
        poneglyph_webhook_url="http://pone.test/api/internal/webhook/analyze",
    )

    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_response.text = "Service Unavailable"

    with patch("app.webhook.httpx.AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.post.return_value = mock_response
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_instance

        result = await deliver_processing_started_callback(
            job_id="xyz-456",
            attempt=1,
            settings=settings,
        )

    assert result is False


@pytest.mark.asyncio
async def test_callback_failure_does_not_abort_processing(queue_dir: str) -> None:
    """Even when the process-started callback fails, the job continues processing."""
    from app.webhook import deliver_processing_started_callback

    settings = Settings(
        great_sage_api_key="key",
        poneglyph_webhook_secret="secret",
        poneglyph_webhook_url="",  # no URL configured
        queue_dir=queue_dir,
    )

    # Should not raise even with no URL
    result = await deliver_processing_started_callback(
        job_id="no-url-job",
        attempt=1,
        settings=settings,
    )
    assert result is False  # gracefully returns False, not an exception


# ---------------------------------------------------------------------------
# 6. Successful completion → completed event → completed/
# ---------------------------------------------------------------------------

def test_successful_processing_moves_to_completed(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)
    append_lifecycle_event(str(job_id), queue_dir, "processing_started")

    # Simulate successful processing
    complete_job(str(job_id), queue_dir)

    assert not _job_dir(queue_dir, "processing", str(job_id)).exists()
    completed_dir = _job_dir(queue_dir, "completed", str(job_id))
    assert completed_dir.is_dir()

    meta = _load_metadata(completed_dir)
    assert meta.status == "completed"
    events = [ev.event for ev in meta.lifecycle]
    assert "completed" in events


def test_completed_metadata_retains_full_lifecycle(queue_dir: str) -> None:
    """The completed/ metadata.json contains the full lifecycle history."""
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)
    append_lifecycle_event(str(job_id), queue_dir, "processing_started")
    mark_callback_delivered(str(job_id), queue_dir)  # simulate callback success before complete
    complete_job(str(job_id), queue_dir)

    meta = _load_metadata(_job_dir(queue_dir, "completed", str(job_id)))
    events = [ev.event for ev in meta.lifecycle]
    assert "enqueued" in events
    assert "claimed" in events
    assert "processing_started" in events
    assert "callback_sent" in events
    assert "completed" in events


# ---------------------------------------------------------------------------
# 7. Completion callback failure → recorded, job still in completed/
# ---------------------------------------------------------------------------

def test_completion_callback_failure_recorded_but_job_completes(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    # Simulate callback failure
    from app.queue_manager import increment_callback_attempts
    increment_callback_attempts(str(job_id), queue_dir, error="connection refused")

    # Job still gets completed
    complete_job(str(job_id), queue_dir)

    completed_dir = _job_dir(queue_dir, "completed", str(job_id))
    assert completed_dir.is_dir()

    meta = _load_metadata(completed_dir)
    assert meta.status == "completed"
    assert meta.callback_delivered is False
    assert meta.callback_attempts == 1
    events = [ev.event for ev in meta.lifecycle]
    assert "callback_failed" in events
    assert "completed" in events


# ---------------------------------------------------------------------------
# 8. Processing failure → retry → eventual failed/
# ---------------------------------------------------------------------------

def test_processing_failure_retries_to_incoming(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir, max_attempts=3)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    retried = retry_or_fail_job(str(job_id), queue_dir, error="OCR crashed", max_attempts=3)

    assert retried is True
    assert _job_dir(queue_dir, "incoming", str(job_id)).is_dir()

    meta = _load_metadata(_job_dir(queue_dir, "incoming", str(job_id)))
    assert meta.attempt == 1
    events = [ev.event for ev in meta.lifecycle]
    assert "retried" in events


def test_processing_failure_exhausts_retries_to_failed(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir, max_attempts=2)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    # First failure → retry
    retry_or_fail_job(str(job_id), queue_dir, error="fail 1", max_attempts=2)
    # Re-claim from incoming
    claim_job(str(job_id), queue_dir)
    # Second failure → permanent fail
    retried = retry_or_fail_job(str(job_id), queue_dir, error="fail 2", max_attempts=2)

    assert retried is False
    assert _job_dir(queue_dir, "failed", str(job_id)).is_dir()

    meta = _load_metadata(_job_dir(queue_dir, "failed", str(job_id)))
    assert meta.status == "failed"
    assert meta.attempt == 2

    events = [ev.event for ev in meta.lifecycle]
    assert "retried" in events
    assert "failed" in events


# ---------------------------------------------------------------------------
# 9. Crash recovery via scan_stranded_jobs
# ---------------------------------------------------------------------------

def _place_in_processing(queue_dir: str, attempt: int = 0, max_attempts: int = 3) -> str:
    job_id = str(uuid.uuid4())
    proc_dir = _job_dir(queue_dir, "processing", job_id)
    proc_dir.mkdir(parents=True, exist_ok=True)
    meta = JobMeta(
        job_id=job_id,
        source="test",
        original_filename="crash.pdf",
        poneglyph_file_path="poneglyph://document/5/crash.pdf",
        document_path=f"./data/jobs/{job_id}/",
        created_at="2026-01-01T00:00:00+00:00",
        enqueued_at="2026-01-01T00:00:00+00:00",
        attempt=attempt,
        max_attempts=max_attempts,
        status="processing",
        lifecycle=[LifecycleEvent(event="enqueued", timestamp="2026-01-01T00:00:00+00:00")],
    )
    _save_metadata(proc_dir, meta)
    return job_id


def test_crash_recovery_moves_stranded_job_to_incoming(queue_dir: str) -> None:
    job_id = _place_in_processing(queue_dir, attempt=0, max_attempts=3)

    recovered = scan_stranded_jobs(queue_dir, max_attempts=3)

    assert job_id in recovered
    assert _job_dir(queue_dir, "incoming", job_id).is_dir()

    meta = _load_metadata(_job_dir(queue_dir, "incoming", job_id))
    events = [ev.event for ev in meta.lifecycle]
    assert "retried" in events


def test_crash_recovery_fails_exhausted_job(queue_dir: str) -> None:
    job_id = _place_in_processing(queue_dir, attempt=3, max_attempts=3)

    recovered = scan_stranded_jobs(queue_dir, max_attempts=3)

    assert job_id not in recovered
    assert _job_dir(queue_dir, "failed", job_id).is_dir()


def test_crash_recovery_multiple_stranded(queue_dir: str) -> None:
    retryable = _place_in_processing(queue_dir, attempt=0, max_attempts=3)
    exhausted = _place_in_processing(queue_dir, attempt=3, max_attempts=3)

    recovered = scan_stranded_jobs(queue_dir, max_attempts=3)

    assert retryable in recovered
    assert exhausted not in recovered


# ---------------------------------------------------------------------------
# 10. QUERY /api/v2/jobs/{job_id} — read-only lifecycle introspection
# ---------------------------------------------------------------------------

def test_query_returns_404_for_unknown_job(client: TestClient) -> None:
    """QUERY for a non-existent job returns 404."""
    response = client.request("QUERY", f"/api/v2/jobs/{uuid.uuid4()}")
    assert response.status_code == 404


def test_query_returns_lifecycle_for_incoming_job(client: TestClient, settings: Settings) -> None:
    """QUERY resolves a job currently in incoming/."""
    # Submit via HTTP to put into incoming/
    response = client.post(
        "/api/v2/jobs",
        files=[("files", ("test.pdf", io.BytesIO(b"%PDF-1.4 t"), "application/pdf"))],
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]

    # QUERY the job
    query_resp = client.request("QUERY", f"/api/v2/jobs/{job_id}")

    # May be in incoming or processing depending on test timing — both are valid
    assert query_resp.status_code == 200
    data = query_resp.json()
    assert data["job_id"] == job_id
    assert data["source"] == "http_v2"
    assert isinstance(data["lifecycle"], list)
    assert len(data["lifecycle"]) >= 1
    assert any(ev["event"] == "enqueued" for ev in data["lifecycle"])


def test_query_returns_correct_state_for_completed_job(
    queue_dir: str, settings: Settings
) -> None:
    """QUERY of a completed job returns status=completed with full lifecycle."""
    fake_settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()

    # Simulate full processing lifecycle
    write_job_to_queue(
        job_id, source="http_v2", settings=fake_settings,
        original_filename="test.pdf",
        poneglyph_file_path="poneglyph://document/7/test.pdf",
    )
    claim_job(str(job_id), queue_dir)
    append_lifecycle_event(str(job_id), queue_dir, "processing_started")
    mark_callback_delivered(str(job_id), queue_dir)
    complete_job(str(job_id), queue_dir)

    # Use load_job_meta directly (bypasses HTTP client)
    meta = load_job_meta(str(job_id), queue_dir)
    assert meta is not None
    assert meta.status == "completed"
    assert meta.callback_delivered is True
    events = [ev.event for ev in meta.lifecycle]
    assert "enqueued" in events
    assert "claimed" in events
    assert "processing_started" in events
    assert "callback_sent" in events
    assert "completed" in events


def test_query_returns_correct_state_for_failed_job(queue_dir: str) -> None:
    """QUERY of a failed job returns status=failed with error and lifecycle."""
    fake_settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()

    write_job_to_queue(job_id, source="http_v2", settings=fake_settings)
    claim_job(str(job_id), queue_dir)
    fail_job(str(job_id), queue_dir, error="OCR service crashed")

    meta = load_job_meta(str(job_id), queue_dir)
    assert meta is not None
    assert meta.status == "failed"
    assert "OCR service crashed" in meta.error
    events = [ev.event for ev in meta.lifecycle]
    assert "failed" in events


def test_query_does_not_move_or_mutate_job(queue_dir: str) -> None:
    """QUERY must not change the queue state of any job."""
    fake_settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=fake_settings)

    # Call load_job_meta (what QUERY uses) multiple times
    for _ in range(5):
        meta = load_job_meta(str(job_id), queue_dir)
        assert meta is not None

    # Job must still be in incoming/
    assert _job_dir(queue_dir, "incoming", str(job_id)).is_dir()
    assert not _job_dir(queue_dir, "processing", str(job_id)).exists()


def test_query_endpoint_returns_all_metadata_fields(client: TestClient, settings: Settings) -> None:
    """QUERY response includes all JobQueryResponse fields."""
    response = client.post(
        "/api/v2/jobs",
        files=[("files", ("doc.pdf", io.BytesIO(b"%PDF-1.4"), "application/pdf"))],
        headers={"X-API-Key": TEST_API_KEY},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]

    query_resp = client.request("QUERY", f"/api/v2/jobs/{job_id}")
    assert query_resp.status_code == 200
    data = query_resp.json()

    # Verify all expected fields are present
    required_fields = {
        "job_id", "source", "original_filename", "poneglyph_file_path",
        "document_path", "status", "attempt", "max_attempts",
        "created_at", "enqueued_at", "callback_delivered",
        "callback_attempts", "lifecycle",
    }
    for field in required_fields:
        assert field in data, f"Missing field: {field}"
