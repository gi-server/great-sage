"""
Unit tests for app.queue_manager (new two-folder architecture).

Tests cover:
- ensure_queue_dirs creates only tmp/, intake/, completed/
- write_job_to_queue writes the raw file to intake/, cleans up tmp/
- move_to_completed moves intake/<job_id>/ → completed/<job_id>/
- get_job_dir finds a job in intake/ or completed/
- get_file_path finds a file in intake/ or completed/
- delete_job_dir removes the job directory from whichever subfolder
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from app.queue_manager import (
    ensure_queue_dirs,
    write_job_to_queue,
    move_to_completed,
    get_job_dir,
    get_file_path,
    delete_job_dir,
    _job_dir,
    _SUBDIRS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def queue_dir(tmp_path: Path) -> str:
    """Return a temporary queue root directory with subdirs created."""
    q = str(tmp_path / "queue")
    ensure_queue_dirs(q)
    return q


class _FakeSettings:
    def __init__(self, queue_dir: str):
        self.queue_dir = queue_dir


SAMPLE_CONTENT = b"%PDF-1.4 fake pdf content"
SAMPLE_FILENAME = "test_doc.pdf"


# ---------------------------------------------------------------------------
# Tests: ensure_queue_dirs
# ---------------------------------------------------------------------------

def test_ensure_queue_dirs_creates_subdirs(tmp_path: Path) -> None:
    q = str(tmp_path / "queue")
    ensure_queue_dirs(q)
    for sub in _SUBDIRS:
        assert (Path(q) / sub).is_dir(), f"Expected {sub}/ to exist"


def test_ensure_queue_dirs_does_not_create_processing_or_failed(tmp_path: Path) -> None:
    q = str(tmp_path / "queue")
    ensure_queue_dirs(q)
    assert not (Path(q) / "processing").exists(), "processing/ should NOT be created"
    assert not (Path(q) / "failed").exists(), "failed/ should NOT be created"
    assert not (Path(q) / "incoming").exists(), "incoming/ should NOT be created"


def test_ensure_queue_dirs_idempotent(queue_dir: str) -> None:
    ensure_queue_dirs(queue_dir)  # second call — should not raise
    for sub in _SUBDIRS:
        assert (Path(queue_dir) / sub).is_dir()


# ---------------------------------------------------------------------------
# Tests: write_job_to_queue
# ---------------------------------------------------------------------------

def test_write_job_to_queue_creates_intake_dir(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    intake_dir = _job_dir(queue_dir, "intake", str(job_id))
    assert intake_dir.is_dir(), "intake/<job_id>/ should exist"


def test_write_job_to_queue_file_content_written(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    file_path = _job_dir(queue_dir, "intake", str(job_id)) / SAMPLE_FILENAME
    assert file_path.exists(), "Raw file should be in intake/<job_id>/"
    assert file_path.read_bytes() == SAMPLE_CONTENT


def test_write_job_to_queue_no_metadata_json(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    intake_dir = _job_dir(queue_dir, "intake", str(job_id))
    assert not (intake_dir / "metadata.json").exists(), "No metadata.json should be written"


def test_write_job_to_queue_no_tmp_left_behind(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    tmp_dir = _job_dir(queue_dir, "tmp", str(job_id))
    assert not tmp_dir.exists(), "tmp/<job_id>/ should be cleaned up after rename"


# ---------------------------------------------------------------------------
# Tests: move_to_completed
# ---------------------------------------------------------------------------

def test_move_to_completed_moves_dir(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    move_to_completed(str(job_id), queue_dir)

    assert not _job_dir(queue_dir, "intake", str(job_id)).exists()
    assert _job_dir(queue_dir, "completed", str(job_id)).is_dir()


def test_move_to_completed_file_travels_with_dir(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    move_to_completed(str(job_id), queue_dir)

    file_path = _job_dir(queue_dir, "completed", str(job_id)) / SAMPLE_FILENAME
    assert file_path.exists()
    assert file_path.read_bytes() == SAMPLE_CONTENT


def test_move_to_completed_missing_dir_logs_warning(queue_dir: str) -> None:
    # Should not raise even if intake dir is missing
    move_to_completed("nonexistent-job-id", queue_dir)


# ---------------------------------------------------------------------------
# Tests: get_job_dir
# ---------------------------------------------------------------------------

def test_get_job_dir_finds_intake(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    result = get_job_dir(str(job_id), queue_dir)
    assert result is not None
    assert "intake" in str(result)


def test_get_job_dir_finds_completed(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)
    move_to_completed(str(job_id), queue_dir)

    result = get_job_dir(str(job_id), queue_dir)
    assert result is not None
    assert "completed" in str(result)


def test_get_job_dir_returns_none_when_missing(queue_dir: str) -> None:
    result = get_job_dir("no-such-job", queue_dir)
    assert result is None


# ---------------------------------------------------------------------------
# Tests: get_file_path
# ---------------------------------------------------------------------------

def test_get_file_path_in_intake(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    path = get_file_path(str(job_id), queue_dir, SAMPLE_FILENAME)
    assert path is not None
    assert path.exists()


def test_get_file_path_in_completed(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)
    move_to_completed(str(job_id), queue_dir)

    path = get_file_path(str(job_id), queue_dir, SAMPLE_FILENAME)
    assert path is not None
    assert path.exists()
    assert "completed" in str(path)


def test_get_file_path_returns_none_when_missing(queue_dir: str) -> None:
    path = get_file_path("no-such-job", queue_dir, "file.pdf")
    assert path is None


# ---------------------------------------------------------------------------
# Tests: delete_job_dir
# ---------------------------------------------------------------------------

def test_delete_job_dir_from_intake(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)

    result = delete_job_dir(str(job_id), queue_dir)
    assert result is True
    assert not _job_dir(queue_dir, "intake", str(job_id)).exists()


def test_delete_job_dir_from_completed(queue_dir: str) -> None:
    job_id = uuid.uuid4()
    settings = _FakeSettings(queue_dir)
    write_job_to_queue(job_id, SAMPLE_CONTENT, SAMPLE_FILENAME, settings)
    move_to_completed(str(job_id), queue_dir)

    result = delete_job_dir(str(job_id), queue_dir)
    assert result is True
    assert not _job_dir(queue_dir, "completed", str(job_id)).exists()


def test_delete_job_dir_returns_false_when_missing(queue_dir: str) -> None:
    result = delete_job_dir("no-such-job", queue_dir)
    assert result is False
