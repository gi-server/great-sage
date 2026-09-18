"""
Async background worker pool for document processing.

Architecture
------------
The Worker owns an `asyncio.Queue[str]` that carries job_id strings.
It spawns `worker_pool_size` concurrent consumer coroutines, each of which:

  1. Waits for a job_id on the channel.
  2. Claims the job via SQLite atomic UPDATE (status: pending → processing).
     Only one worker wins — the rest skip.
  3. Appends a 'claimed' JobEvent to SQLite.
  4. Fires a process-started HTTP callback to Poneglyph (best-effort).
  5. Calls pipeline.process_job() — the core OCR/LLM pipeline.
  6. On success: moves job folder intake/ → completed/, updates SQLite.
  7. On failure: retries up to max_attempts (logged to terminal only),
     then fires a failure webhook and marks SQLite status='failed'.
     File stays in intake/ on failure.

The QueueWatcher feeds the channel from filesystem events on intake/.
HTTP handlers write the file to intake/ via queue_manager, which the
watcher detects and pushes onto the internal channel.

Crash recovery
--------------
On startup, any SQLite Job rows stuck in status='processing' from a
previous crashed run are reset to 'pending'.  The watcher will detect
their directories in intake/ and re-queue them.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional

from sqlmodel import Session, select

from app import queue_manager
from app.database import engine
from app.models import Job, JobEvent
from app.pipeline import process_job
from app.webhook import deliver_processing_started_callback

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.worker")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _write_event(
    session: Session,
    job: Job,
    event: str,
    file_status: str,
    raw_data: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    """Append a JobEvent row and commit."""
    ev = JobEvent(
        job_id=job.id,
        event=event,
        file_status=file_status,
        source=job.source,
        raw_data=raw_data,
        detail=detail,
        timestamp=_utcnow(),
    )
    session.add(ev)
    session.commit()


class Worker:
    """SQLite-backed async worker pool."""

    def __init__(self, settings: "Settings") -> None:
        self._settings = settings
        self._queue: asyncio.Queue[str] = asyncio.Queue(
            maxsize=settings.worker_queue_size,
        )
        self._tasks: List[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the worker pool."""
        n = self._settings.worker_pool_size
        for i in range(n):
            task = asyncio.create_task(
                self._consume(worker_id=i),
                name=f"great-sage-worker-{i}",
            )
            self._tasks.append(task)
        logger.info(
            "Worker pool started (%d workers, queue capacity=%d)",
            n, self._settings.worker_queue_size,
        )

    async def stop(self) -> None:
        """Cancel all worker tasks and wait for them to finish."""
        for task in self._tasks:
            task.cancel()
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                logger.error("Worker task error during shutdown: %s", r)
        self._tasks.clear()
        logger.info("Worker pool stopped")

    # ------------------------------------------------------------------
    # Enqueue helpers (public API)
    # ------------------------------------------------------------------

    async def enqueue_job_id_with_meta(
        self,
        job_id: uuid.UUID,
        file_content: bytes,
        filename: str,
        source: str = "unknown",
        callback_url: Optional[str] = None,
    ) -> None:
        """
        Write the job file to the filesystem queue (intake/).

        The QueueWatcher will detect the new directory in intake/ and
        push job_id onto the internal channel automatically.
        """
        queue_manager.write_job_to_queue(
            job_id=job_id,
            file_content=file_content,
            filename=filename,
            settings=self._settings,
        )
        logger.info(
            "Job %s written to queue/intake/ (source=%s, file=%s)",
            job_id, source, filename,
        )

    def push_to_channel(self, job_id: str) -> None:
        """
        Push a job_id string directly onto the internal asyncio channel.

        Called by QueueWatcher (from the event loop thread via call_soon_threadsafe)
        and by crash-recovery code on startup.
        """
        try:
            self._queue.put_nowait(job_id)
        except asyncio.QueueFull:
            logger.warning(
                "Internal channel full — job %s will be recovered on next restart "
                "(it remains in queue/intake/)",
                job_id,
            )

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    async def _consume(self, worker_id: int) -> None:
        """Long-lived worker coroutine. Processes one job at a time."""
        logger.info("Worker %d started", worker_id)
        while True:
            try:
                item = await self._queue.get()
                if isinstance(item, str):
                    await self._process_intake_job(item, worker_id)
                elif isinstance(item, uuid.UUID):
                    await self._process_intake_job(str(item), worker_id)
                self._queue.task_done()
            except asyncio.CancelledError:
                logger.info("Worker %d cancelled", worker_id)
                raise
            except Exception:
                logger.exception("Worker %d: unexpected error in consumer loop", worker_id)
                await asyncio.sleep(1)

    async def _process_intake_job(self, job_id: str, worker_id: int) -> None:
        """Claim, process, and settle a single job from intake/."""
        queue_dir = self._settings.queue_dir
        max_attempts = self._settings.queue_max_attempts

        # ── 1. Atomically claim via SQLite ─────────────────────────────────
        # Only one worker wins — SQLite serialises writes.
        with Session(engine) as session:
            job_uuid = uuid.UUID(job_id)
            job = session.get(Job, job_uuid)

            if job is None:
                logger.warning("Worker %d: job %s not found in SQLite — skipping", worker_id, job_id)
                return

            if job.status != "pending":
                logger.debug(
                    "Worker %d: job %s already %s — skipping (another worker claimed it)",
                    worker_id, job_id, job.status,
                )
                return

            # Claim it
            job.status = "processing"
            job.updated_at = _utcnow()
            session.add(job)
            session.commit()
            session.refresh(job)

            attempt = (job.events.count if hasattr(job.events, "count") else 0)
            _write_event(session, job, "claimed", "processing",
                         detail=f"Worker {worker_id}")

        logger.info("Worker %d claimed job %s", worker_id, job_id)

        # ── 2. Fire process-started callback to Poneglyph (best-effort) ────
        try:
            await deliver_processing_started_callback(
                job_id=job_id,
                attempt=1,
                settings=self._settings,
            )
        except Exception:
            logger.exception("Worker %d: exception during process-started callback for job %s", worker_id, job_id)

        # ── 3. Run OCR → LLM → webhook pipeline (with retry loop) ─────────
        last_error: Optional[str] = None
        attempt_num = 0

        for attempt_num in range(1, max_attempts + 1):
            try:
                result = await process_job(
                    job_id=job_uuid,
                    settings=self._settings,
                )

                if isinstance(result, tuple):
                    _all_success, _callback_ok = result
                else:
                    _all_success = bool(result)
                    _callback_ok = True

                # ── 4a. Success path ────────────────────────────────────────
                queue_manager.move_to_completed(job_id, queue_dir)

                with Session(engine) as session:
                    job = session.get(Job, job_uuid)
                    if job:
                        job.status = "completed"
                        job.updated_at = _utcnow()
                        # Update filepath references in JobFile rows
                        from app.models import JobFile
                        from sqlmodel import select as _select
                        files = session.exec(_select(JobFile).where(JobFile.job_id == job_uuid)).all()
                        for f in files:
                            # Update filepath to reflect completed/ location
                            f.filepath = f.filepath.replace(
                                f"queue/intake/{job_id}",
                                f"queue/completed/{job_id}",
                            )
                            session.add(f)
                        session.add(job)
                        session.commit()
                        session.refresh(job)
                        _write_event(session, job, "completed", "completed",
                                     raw_data=job.raw_data)

                logger.info("Worker %d completed job %s", worker_id, job_id)
                return

            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Worker %d: job %s failed on attempt %d/%d — %s",
                    worker_id, job_id, attempt_num, max_attempts, last_error,
                )

                if attempt_num < max_attempts:
                    # Reset to pending so the next iteration retries
                    with Session(engine) as session:
                        job = session.get(Job, job_uuid)
                        if job:
                            job.status = "pending"
                            job.updated_at = _utcnow()
                            session.add(job)
                            session.commit()
                            session.refresh(job)
                            _write_event(session, job, "retried", "pending",
                                         detail=f"Attempt {attempt_num}/{max_attempts}: {last_error[:300]}")
                    # Brief back-off before retry
                    await asyncio.sleep(2 ** attempt_num)
                    # Re-claim for the next attempt
                    with Session(engine) as session:
                        job = session.get(Job, job_uuid)
                        if job and job.status == "pending":
                            job.status = "processing"
                            job.updated_at = _utcnow()
                            session.add(job)
                            session.commit()

        # ── 4b. Failure path (exhausted all attempts) ──────────────────────
        with Session(engine) as session:
            job = session.get(Job, job_uuid)
            if job:
                job.status = "failed"
                job.updated_at = _utcnow()
                session.add(job)
                session.commit()
                session.refresh(job)
                _write_event(session, job, "failed", "failed",
                             detail=f"Exhausted {attempt_num} attempt(s). Last error: {(last_error or 'unknown')[:400]}")

        logger.error(
            "Worker %d: job %s permanently failed after %d attempt(s): %s",
            worker_id, job_id, attempt_num, last_error,
        )

        # Fire failure webhook to Poneglyph
        try:
            from app.schemas import WebhookPayload, ClassificationResult
            from app.webhook import deliver_webhook
            with Session(engine) as session:
                job = session.get(Job, job_uuid)
                if job and job.legacy_document_id:
                    payload = WebhookPayload(
                        document_id=job.legacy_document_id,
                        status="failed",
                        ocr_text=None,
                        classification=ClassificationResult(),
                        error_message=last_error,
                    )
                    await deliver_webhook(payload, self._settings)
        except Exception:
            logger.exception("Worker %d: failed to deliver failure webhook for job %s", worker_id, job_id)
