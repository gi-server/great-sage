"""
Unit tests for app.queue_manager.

Tests cover:
- ensure_queue_dirs creates all subdirectories
- write_job_to_queue writes metadata.json and places dir in incoming/
- claim_job atomically moves incoming/ → processing/
- claim_job returns False if job already claimed
- complete_job moves processing/ → completed/
- fail_job moves processing/ → failed/
- retry_or_fail_job retries when attempt < max_attempts
- retry_or_fail_job permanently fails when attempt >= max_attempts
- scan_stranded_jobs recovers retryable jobs and permanently fails exhausted ones
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from app.queue_manager import (
    JobMeta,
    claim_job,
    complete_job,
    ensure_queue_dirs,
    fail_job,
    load_job_meta,
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

@pytest.fixture()
def queue_dir(tmp_path: Path) -> str:
    """Return a temporary queue root directory."""
    q = str(tmp_path / "queue")
    ensure_queue_dirs(q)
    return q


class _FakeSettings:
    def __init__(self, queue_dir: str, max_attempts: int = 3):
        self.queue_dir = queue_dir
        self.queue_max_attempts = max_attempts


# ---------------------------------------------------------------------------
# Tests: ensure_queue_dirs
# ---------------------------------------------------------------------------

def test_ensure_queue_dirs_creates_subdirs(tmp_path: Path) -> None:
    q = str(tmp_path / "queue")
    ensure_queue_dirs(q)
    for sub in ("tmp", "incoming", "processing", "completed", "failed"):
        assert (Path(q) / sub).is_dir(), f"Missing subdir: {sub}"


def test_ensure_queue_dirs_idempotent(queue_dir: str) -> None:
    # Calling again must not raise
    ensure_queue_dirs(queue_dir)
    for sub in ("tmp", "incoming", "processing", "completed", "failed"):
        assert (Path(queue_dir) / sub).is_dir()


# ---------------------------------------------------------------------------
# Tests: write_job_to_queue
# ---------------------------------------------------------------------------

def test_write_job_to_queue_creates_incoming_dir(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)

    job_dir = _job_dir(queue_dir, "incoming", str(job_id))
    assert job_dir.is_dir()
    assert (job_dir / "metadata.json").is_file()


def test_write_job_to_queue_metadata_fields(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir, max_attempts=5)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v1", settings=settings, person_id="person-abc")

    job_dir = _job_dir(queue_dir, "incoming", str(job_id))
    meta = _load_metadata(job_dir)
    assert meta is not None
    assert meta.job_id == str(job_id)
    assert meta.source == "http_v1"
    assert meta.person_id == "person-abc"
    assert meta.attempt == 0
    assert meta.max_attempts == 5
    assert meta.status == "pending"


def test_write_job_to_queue_no_tmp_left_behind(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)

    # tmp/ staging dir must have been cleaned up by the rename
    tmp_dir = _job_dir(queue_dir, "tmp", str(job_id))
    assert not tmp_dir.exists()


# ---------------------------------------------------------------------------
# Tests: claim_job
# ---------------------------------------------------------------------------

def test_claim_job_moves_to_processing(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)

    claimed = claim_job(str(job_id), queue_dir)
    assert claimed is True

    assert not _job_dir(queue_dir, "incoming", str(job_id)).exists()
    assert _job_dir(queue_dir, "processing", str(job_id)).is_dir()


def test_claim_job_returns_false_when_already_claimed(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)

    assert claim_job(str(job_id), queue_dir) is True
    # Second claim attempt must fail gracefully
    assert claim_job(str(job_id), queue_dir) is False


# ---------------------------------------------------------------------------
# Tests: complete_job
# ---------------------------------------------------------------------------

def test_complete_job_moves_to_completed(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    complete_job(str(job_id), queue_dir)

    assert not _job_dir(queue_dir, "processing", str(job_id)).exists()
    completed_dir = _job_dir(queue_dir, "completed", str(job_id))
    assert completed_dir.is_dir()
    meta = _load_metadata(completed_dir)
    assert meta is not None
    assert meta.status == "completed"


# ---------------------------------------------------------------------------
# Tests: fail_job
# ---------------------------------------------------------------------------

def test_fail_job_moves_to_failed_with_error(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    fail_job(str(job_id), queue_dir, error="something exploded")

    failed_dir = _job_dir(queue_dir, "failed", str(job_id))
    assert failed_dir.is_dir()
    meta = _load_metadata(failed_dir)
    assert meta is not None
    assert meta.status == "failed"
    assert "something exploded" in meta.error


# ---------------------------------------------------------------------------
# Tests: retry_or_fail_job
# ---------------------------------------------------------------------------

def test_retry_or_fail_job_retries_when_under_limit(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir, max_attempts=3)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    retried = retry_or_fail_job(str(job_id), queue_dir, error="oops", max_attempts=3)

    assert retried is True
    assert _job_dir(queue_dir, "incoming", str(job_id)).is_dir()
    assert not _job_dir(queue_dir, "processing", str(job_id)).exists()

    # Attempt counter must be incremented
    meta = _load_metadata(_job_dir(queue_dir, "incoming", str(job_id)))
    assert meta is not None
    assert meta.attempt == 1


def test_retry_or_fail_job_permanently_fails_when_exhausted(queue_dir: str) -> None:
    settings = _FakeSettings(queue_dir, max_attempts=1)
    job_id = uuid.uuid4()
    write_job_to_queue(job_id, source="http_v2", settings=settings)
    claim_job(str(job_id), queue_dir)

    # Manually set attempt to max in metadata before calling
    proc_dir = _job_dir(queue_dir, "processing", str(job_id))
    meta = _load_metadata(proc_dir)
    meta.attempt = 0  # will be incremented to 1 inside retry_or_fail_job
    meta.max_attempts = 1
    _save_metadata(proc_dir, meta)

    retried = retry_or_fail_job(str(job_id), queue_dir, error="terminal", max_attempts=1)

    assert retried is False
    assert _job_dir(queue_dir, "failed", str(job_id)).is_dir()


# ---------------------------------------------------------------------------
# Tests: scan_stranded_jobs
# ---------------------------------------------------------------------------

def _place_job_in_processing(queue_dir: str, attempt: int, max_attempts: int) -> str:
    """Helper: create a job directory directly in processing/ with given attempt count."""
    job_id = str(uuid.uuid4())
    proc_dir = _job_dir(queue_dir, "processing", job_id)
    proc_dir.mkdir(parents=True, exist_ok=True)
    meta = JobMeta(
        job_id=job_id,
        document_path=f"./data/jobs/{job_id}/",
        source="test",
        created_at="2026-01-01T00:00:00+00:00",
        enqueued_at="2026-01-01T00:00:00+00:00",
        attempt=attempt,
        max_attempts=max_attempts,
        status="processing",
    )
    _save_metadata(proc_dir, meta)
    return job_id


def test_scan_stranded_jobs_recovers_retryable(queue_dir: str) -> None:
    job_id = _place_job_in_processing(queue_dir, attempt=0, max_attempts=3)

    recovered = scan_stranded_jobs(queue_dir, max_attempts=3)

    assert job_id in recovered
    assert _job_dir(queue_dir, "incoming", job_id).is_dir()
    assert not _job_dir(queue_dir, "processing", job_id).exists()


def test_scan_stranded_jobs_fails_exhausted(queue_dir: str) -> None:
    job_id = _place_job_in_processing(queue_dir, attempt=3, max_attempts=3)

    recovered = scan_stranded_jobs(queue_dir, max_attempts=3)

    assert job_id not in recovered
    assert _job_dir(queue_dir, "failed", job_id).is_dir()


def test_scan_stranded_jobs_empty_processing(queue_dir: str) -> None:
    recovered = scan_stranded_jobs(queue_dir, max_attempts=3)
    assert recovered == []
