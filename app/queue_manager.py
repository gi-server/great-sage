"""
Filesystem-backed queue manager for Great Sage.

The queue is two directories under `data/queue/`:

    data/queue/
        tmp/        ← staging; write file here, then rename to intake/ atomically
        intake/     ← jobs waiting to be picked up (and active during processing)
        completed/  ← successfully processed jobs

Each queue entry is a directory named by job_id containing only the raw
document file(s).  All job state (status, source, events, OCR text) lives
in SQLite — there is no metadata.json on disk.

Lifecycle:
  1. File written to tmp/<job_id>/<filename>, renamed atomically to intake/<job_id>/
  2. Worker claims via SQLite atomic UPDATE (status: pending → processing)
  3. OCR + LLM run — file stays in intake/<job_id>/ throughout
  4. On success: os.rename(intake/<job_id>/, completed/<job_id>/)
  5. On failure: file stays in intake/<job_id>/; SQLite status = 'failed'

All state transitions that matter are in SQLite.  The folder rename is
only used once — on job completion — to give operators a clean
completed/ directory to inspect.
"""

from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.queue_manager")


# ---------------------------------------------------------------------------
# Queue sub-directory names
# ---------------------------------------------------------------------------

_SUBDIRS = ("tmp", "intake", "completed")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _queue_subdir(queue_dir: str, subdir: str) -> Path:
    return Path(queue_dir) / subdir


def _job_dir(queue_dir: str, subdir: str, job_id: str) -> Path:
    return _queue_subdir(queue_dir, subdir) / job_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_queue_dirs(queue_dir: str) -> None:
    """Create all queue sub-directories if they do not already exist."""
    for sub in _SUBDIRS:
        path = _queue_subdir(queue_dir, sub)
        path.mkdir(parents=True, exist_ok=True)
    logger.info("Queue directories ready under %s", queue_dir)


def write_job_to_queue(
    job_id: uuid.UUID,
    file_content: bytes,
    filename: str,
    settings: "Settings",
) -> None:
    """
    Publish a job onto the filesystem queue.

    Writes the raw file bytes to `tmp/<job_id>/<filename>` first, then
    atomically renames the directory to `intake/<job_id>/` so the watcher
    never sees a partially written file.

    No metadata.json is written — all job state is in SQLite.

    Args:
        job_id:       The UUID of the job (matches the SQLite Job record).
        file_content: Raw bytes of the uploaded document.
        filename:     Original filename (used for the on-disk file name).
        settings:     Application settings (provides queue_dir).
    """
    queue_dir = settings.queue_dir
    job_id_str = str(job_id)

    # Write to tmp/ staging directory
    tmp_dir = _job_dir(queue_dir, "tmp", job_id_str)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    file_path = tmp_dir / filename
    file_path.write_bytes(file_content)

    # Atomic rename into intake/
    intake_dir = _job_dir(queue_dir, "intake", job_id_str)
    os.rename(tmp_dir, intake_dir)
    logger.info("Job %s published to queue/intake/ (file=%s)", job_id_str, filename)


def move_to_completed(job_id: str, queue_dir: str) -> None:
    """
    Atomically move a completed job from `intake/` to `completed/`.

    Called by the worker after the job has been fully processed.
    The file(s) travel with the directory — no copying.
    """
    intake = _job_dir(queue_dir, "intake", job_id)
    completed = _job_dir(queue_dir, "completed", job_id)

    if intake.exists():
        os.rename(intake, completed)
        logger.info("Job %s → completed/", job_id)
    else:
        logger.warning("move_to_completed: intake dir not found for job %s", job_id)


def get_job_dir(job_id: str, queue_dir: str) -> Optional[Path]:
    """
    Return the current on-disk directory for a job, regardless of which
    queue subfolder it is currently in.

    Searches intake/ first (most common), then completed/.
    Returns None if the job directory does not exist on disk.
    """
    for sub in ("intake", "completed"):
        job_dir = _job_dir(queue_dir, sub, job_id)
        if job_dir.exists():
            return job_dir
    return None


def delete_job_dir(job_id: str, queue_dir: str) -> bool:
    """
    Delete a job's on-disk directory from whichever queue subfolder it
    currently lives in.

    Returns True if a directory was found and deleted, False otherwise.
    """
    import shutil
    for sub in ("intake", "completed"):
        job_dir = _job_dir(queue_dir, sub, job_id)
        if job_dir.exists():
            shutil.rmtree(job_dir)
            logger.info("Job %s directory deleted from %s/", job_id, sub)
            return True
    logger.warning("delete_job_dir: no directory found for job %s", job_id)
    return False


def get_file_path(job_id: str, queue_dir: str, filename: str) -> Optional[Path]:
    """
    Return the absolute path to a specific file inside a job's directory.

    Searches intake/ first (file lives here during processing), then
    completed/ (after a successful run).  Returns None if not found.
    """
    for sub in ("intake", "completed"):
        candidate = _job_dir(queue_dir, sub, job_id) / filename
        if candidate.exists():
            return candidate
    return None
