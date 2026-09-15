"""
Filesystem-backed queue manager for Great Sage.

The queue is a set of directories under `data/queue/`:

    data/queue/
        tmp/         ← staging; write here, then rename to incoming/ atomically
        incoming/    ← jobs waiting to be picked up by the watcher
        processing/  ← jobs claimed by a worker (renamed atomically from incoming/)
        completed/   ← successfully processed jobs
        failed/      ← jobs that exhausted retries

Each queue entry is a directory named by job_id.  It contains only
`metadata.json` — the actual document files remain in `data/jobs/<job_id>/`
so nothing is ever duplicated.  Poneglyph owns the original file permanently;
Great Sage's working copy under `data/jobs/` is transient OCR input only.

All state transitions are atomic OS renames on the same filesystem volume,
which is guaranteed because all subdirectories share the same `queue_dir`
root under `data/`.

Lifecycle events are appended to metadata.json at each state transition,
providing a full chronological audit trail for every job.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.queue_manager")


# ---------------------------------------------------------------------------
# Queue sub-directory names
# ---------------------------------------------------------------------------

_SUBDIRS = ("tmp", "incoming", "processing", "completed", "failed")


# ---------------------------------------------------------------------------
# Lifecycle event
# ---------------------------------------------------------------------------

@dataclass
class LifecycleEvent:
    """A single timestamped event in a job's processing history."""

    event: str                    # enqueued | claimed | processing_started | completed | failed | retried | callback_sent | callback_failed
    timestamp: str                # ISO8601 UTC
    detail: Optional[str] = None  # optional human-readable context


# ---------------------------------------------------------------------------
# Metadata schema
# ---------------------------------------------------------------------------

@dataclass
class JobMeta:
    """Envelope written to each queue job directory as metadata.json."""

    job_id: str
    source: str                       # "http_v1" | "http_v2"
    original_filename: str            # submitted filename (may be "<docID>_<name>" for V2 batch)
    poneglyph_file_path: str          # canonical Poneglyph-side file reference (filename or path)
    document_path: str                # Great Sage working dir: data/jobs/<job_id>/
    created_at: str
    enqueued_at: str
    status: str = "pending"           # pending | processing | completed | failed
    attempt: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    callback_url: Optional[str] = None      # webhook_url from the originating job record
    callback_delivered: bool = False        # True once a completion callback 2xx was received
    callback_attempts: int = 0             # total completion-callback delivery attempts
    lifecycle: List[LifecycleEvent] = field(default_factory=list)


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
    source: str,
    settings: "Settings",
    original_filename: str = "",
    poneglyph_file_path: str = "",
    callback_url: Optional[str] = None,
) -> None:
    """
    Publish a job onto the filesystem queue.

    Writes metadata.json to `tmp/<job_id>/` first, then atomically
    renames the directory to `incoming/<job_id>/` so the watcher never
    sees a partially written entry.

    Args:
        job_id:               The UUID of the job (matches the SQLite Job record).
        source:               "http_v1" or "http_v2".
        settings:             Application settings (provides queue_dir, max_attempts).
        original_filename:    The submitted filename, used for lifecycle audit trail.
        poneglyph_file_path:  Canonical reference to the Poneglyph-owned file.
        callback_url:         The webhook_url to call on completion.
    """
    queue_dir = settings.queue_dir
    job_id_str = str(job_id)
    now = datetime.now(timezone.utc).isoformat()

    enqueued_event = LifecycleEvent(event="enqueued", timestamp=now)

    meta = JobMeta(
        job_id=job_id_str,
        source=source,
        original_filename=original_filename,
        poneglyph_file_path=poneglyph_file_path,
        document_path=f"./data/jobs/{job_id_str}/",
        created_at=now,
        enqueued_at=now,
        attempt=0,
        max_attempts=settings.queue_max_attempts,
        status="pending",
        callback_url=callback_url,
        callback_delivered=False,
        callback_attempts=0,
        lifecycle=[enqueued_event],
    )

    # Write to tmp/ staging directory
    tmp_dir = _job_dir(queue_dir, "tmp", job_id_str)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    _save_metadata(tmp_dir, meta)

    # Atomic rename into incoming/
    incoming_dir = _job_dir(queue_dir, "incoming", job_id_str)
    os.rename(tmp_dir, incoming_dir)
    logger.info("Job %s published to queue/incoming/", job_id_str)


def claim_job(job_id: str, queue_dir: str) -> bool:
    """
    Atomically move a job from `incoming/` to `processing/`.

    Returns True if the claim succeeded, False if the job was already
    claimed by another worker (i.e. the directory was gone from incoming/).
    This is the concurrency-safety gate — only one worker wins the rename.

    Appends a 'claimed' lifecycle event to the metadata on success.
    """
    incoming = _job_dir(queue_dir, "incoming", job_id)
    processing = _job_dir(queue_dir, "processing", job_id)

    try:
        os.rename(incoming, processing)
        logger.info("Job %s claimed → processing/", job_id)
    except FileNotFoundError:
        logger.warning("Job %s already claimed by another worker (not in incoming/)", job_id)
        return False
    except Exception:
        logger.exception("Unexpected error claiming job %s", job_id)
        return False

    # Append lifecycle event after the rename succeeds
    _append_event(processing, LifecycleEvent(
        event="claimed",
        timestamp=datetime.now(timezone.utc).isoformat(),
    ))
    return True


def complete_job(job_id: str, queue_dir: str) -> None:
    """Move a job from `processing/` to `completed/` and update its status."""
    processing = _job_dir(queue_dir, "processing", job_id)
    completed = _job_dir(queue_dir, "completed", job_id)

    if processing.exists():
        meta = _load_metadata(processing)
        if meta:
            meta.status = "completed"
            meta.lifecycle.append(LifecycleEvent(
                event="completed",
                timestamp=datetime.now(timezone.utc).isoformat(),
            ))
            _save_metadata(processing, meta)
        os.rename(processing, completed)
        logger.info("Job %s → completed/", job_id)
    else:
        logger.warning("complete_job: processing dir not found for job %s", job_id)


def fail_job(job_id: str, queue_dir: str, error: str) -> None:
    """
    Move a job from `processing/` to `failed/` and record the error.

    The metadata retains the attempt count so operators can inspect
    why it failed and whether it exhausted all retries.
    """
    processing = _job_dir(queue_dir, "processing", job_id)
    failed = _job_dir(queue_dir, "failed", job_id)

    if processing.exists():
        meta = _load_metadata(processing)
        if meta:
            meta.status = "failed"
            meta.error = error
            meta.lifecycle.append(LifecycleEvent(
                event="failed",
                timestamp=datetime.now(timezone.utc).isoformat(),
                detail=error[:500],
            ))
            _save_metadata(processing, meta)
        os.rename(processing, failed)
        logger.info("Job %s → failed/ (error=%s)", job_id, error[:120])
    else:
        logger.warning("fail_job: processing dir not found for job %s", job_id)


def retry_or_fail_job(job_id: str, queue_dir: str, error: str, max_attempts: int) -> bool:
    """
    After a processing failure, decide whether to retry or permanently fail.

    Increments the attempt counter.  If attempt < max_attempts, moves the job
    back to `incoming/` for the watcher to re-discover.  Otherwise moves it to
    `failed/`.

    Returns True if the job was re-queued for retry, False if permanently failed.
    """
    processing = _job_dir(queue_dir, "processing", job_id)

    if not processing.exists():
        logger.warning("retry_or_fail_job: processing dir not found for job %s", job_id)
        return False

    meta = _load_metadata(processing)
    if meta is None:
        # No metadata — can't retry safely, just fail
        fail_job(job_id, queue_dir, error)
        return False

    meta.attempt += 1
    meta.error = error

    if meta.attempt < meta.max_attempts:
        meta.status = "pending"
        meta.lifecycle.append(LifecycleEvent(
            event="retried",
            timestamp=datetime.now(timezone.utc).isoformat(),
            detail=f"Attempt {meta.attempt}/{meta.max_attempts}: {error[:200]}",
        ))
        _save_metadata(processing, meta)
        incoming = _job_dir(queue_dir, "incoming", job_id)
        os.rename(processing, incoming)
        logger.info(
            "Job %s re-queued for retry (attempt %d/%d)",
            job_id, meta.attempt, meta.max_attempts,
        )
        return True
    else:
        meta.status = "failed"
        meta.lifecycle.append(LifecycleEvent(
            event="failed",
            timestamp=datetime.now(timezone.utc).isoformat(),
            detail=f"Exhausted {meta.attempt} attempts. Last error: {error[:200]}",
        ))
        _save_metadata(processing, meta)
        failed = _job_dir(queue_dir, "failed", job_id)
        os.rename(processing, failed)
        logger.warning(
            "Job %s permanently failed after %d attempts: %s",
            job_id, meta.attempt, error[:120],
        )
        return False


def scan_stranded_jobs(queue_dir: str, max_attempts: int) -> List[str]:
    """
    Scan `processing/` for jobs left there by a previous crashed process.

    For each stranded job:
      - If attempt < max_attempts  → move back to incoming/ for retry
      - If attempt >= max_attempts → move to failed/

    Returns list of job_ids that were moved back to incoming/ (to be logged).
    """
    processing_dir = _queue_subdir(queue_dir, "processing")
    if not processing_dir.exists():
        return []

    retried: List[str] = []

    for entry in processing_dir.iterdir():
        if not entry.is_dir():
            continue

        job_id = entry.name
        meta = _load_metadata(entry)
        attempt = meta.attempt if meta else 0
        effective_max = meta.max_attempts if meta else max_attempts

        error = "Stranded after crash/restart"

        if attempt < effective_max:
            if meta:
                meta.attempt += 1
                meta.status = "pending"
                meta.error = error
                meta.lifecycle.append(LifecycleEvent(
                    event="retried",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    detail=f"Crash recovery — stranded in processing/ (attempt {meta.attempt})",
                ))
                _save_metadata(entry, meta)
            incoming = _job_dir(queue_dir, "incoming", job_id)
            os.rename(entry, incoming)
            logger.info("Stranded job %s recovered → incoming/ (attempt %d)", job_id, attempt + 1)
            retried.append(job_id)
        else:
            if meta:
                meta.status = "failed"
                meta.error = error
                meta.lifecycle.append(LifecycleEvent(
                    event="failed",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    detail="Stranded after crash — exhausted all retry attempts",
                ))
                _save_metadata(entry, meta)
            failed = _job_dir(queue_dir, "failed", job_id)
            os.rename(entry, failed)
            logger.warning("Stranded job %s permanently failed (exhausted attempts)", job_id)

    return retried


def load_job_meta(job_id: str, queue_dir: str) -> Optional[JobMeta]:
    """
    Load metadata for a job currently in any queue state directory.

    Searches processing/ first (most common call site), then others.
    """
    for sub in ("processing", "incoming", "completed", "failed"):
        job_dir = _job_dir(queue_dir, sub, job_id)
        if job_dir.exists():
            return _load_metadata(job_dir)
    return None


def query_jobs(queue_dir: str, status: Optional[str] = None) -> List[JobMeta]:
    """
    Query the queue for jobs, optionally filtering by status.
    If status is provided, it only searches that specific subdirectory
    (e.g., 'processing', 'incoming', 'completed', 'failed').
    Otherwise, it searches all queue subdirectories.
    """
    subdirs_to_search = [status] if status else _SUBDIRS
    jobs: List[JobMeta] = []

    for sub in subdirs_to_search:
        if sub not in _SUBDIRS:
            continue

        subdir_path = _queue_subdir(queue_dir, sub)
        if not subdir_path.exists():
            continue

        for entry in subdir_path.iterdir():
            if entry.is_dir():
                meta = _load_metadata(entry)
                if meta:
                    jobs.append(meta)

    return jobs


# ---------------------------------------------------------------------------
# Lifecycle helpers (called from worker.py)
# ---------------------------------------------------------------------------

def append_lifecycle_event(
    job_id: str,
    queue_dir: str,
    event: str,
    detail: Optional[str] = None,
) -> None:
    """
    Append a lifecycle event to a job's metadata.json in-place.

    Searches all subdirs for the job, appends the event, and saves atomically.
    No-op if the job is not found.
    """
    for sub in ("processing", "incoming", "completed", "failed"):
        job_dir = _job_dir(queue_dir, sub, job_id)
        if job_dir.exists():
            meta = _load_metadata(job_dir)
            if meta:
                meta.lifecycle.append(LifecycleEvent(
                    event=event,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    detail=detail,
                ))
                _save_metadata(job_dir, meta)
            return
    logger.warning("append_lifecycle_event: job %s not found in any queue subdir", job_id)


def mark_callback_delivered(job_id: str, queue_dir: str) -> None:
    """
    Mark the completion callback as successfully delivered in the job metadata.
    Searches all subdirs (job may be in completed/ at this point).
    """
    for sub in ("processing", "completed", "failed", "incoming"):
        job_dir = _job_dir(queue_dir, sub, job_id)
        if job_dir.exists():
            meta = _load_metadata(job_dir)
            if meta:
                meta.callback_delivered = True
                meta.callback_attempts += 1
                meta.lifecycle.append(LifecycleEvent(
                    event="callback_sent",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    detail=f"Completion callback delivered (attempt {meta.callback_attempts})",
                ))
                _save_metadata(job_dir, meta)
            return


def increment_callback_attempts(job_id: str, queue_dir: str, error: Optional[str] = None) -> None:
    """
    Record a failed completion callback attempt without marking it as delivered.
    """
    for sub in ("processing", "completed", "failed", "incoming"):
        job_dir = _job_dir(queue_dir, sub, job_id)
        if job_dir.exists():
            meta = _load_metadata(job_dir)
            if meta:
                meta.callback_attempts += 1
                meta.lifecycle.append(LifecycleEvent(
                    event="callback_failed",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    detail=f"Attempt {meta.callback_attempts}: {(error or 'unknown error')[:300]}",
                ))
                _save_metadata(job_dir, meta)
            return


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_META_FILENAME = "metadata.json"


def _load_metadata(job_dir: Path) -> Optional[JobMeta]:
    meta_path = job_dir / _META_FILENAME
    try:
        with open(meta_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        # Deserialize lifecycle list (list of dicts → list of LifecycleEvent)
        raw_lifecycle = data.pop("lifecycle", [])
        lifecycle = [
            LifecycleEvent(**ev) if isinstance(ev, dict) else ev
            for ev in raw_lifecycle
        ]
        meta = JobMeta(**data)
        meta.lifecycle = lifecycle
        return meta
    except FileNotFoundError:
        logger.warning("metadata.json not found in %s", job_dir)
        return None
    except Exception:
        logger.exception("Failed to read metadata.json from %s", job_dir)
        return None


def _save_metadata(job_dir: Path, meta: JobMeta) -> None:
    meta_path = job_dir / _META_FILENAME
    try:
        data = asdict(meta)
        tmp_path = meta_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp_path, meta_path)  # atomic on Windows and POSIX
    except Exception:
        logger.exception("Failed to write metadata.json to %s", job_dir)


def _append_event(job_dir: Path, event: LifecycleEvent) -> None:
    """Append a single LifecycleEvent to an existing metadata.json."""
    meta = _load_metadata(job_dir)
    if meta:
        meta.lifecycle.append(event)
        _save_metadata(job_dir, meta)
