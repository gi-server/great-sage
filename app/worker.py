"""
Async background worker pool for document processing.

Architecture
------------
The Worker owns an `asyncio.Queue[str]` that carries job_id strings.
It spawns `worker_pool_size` concurrent consumer coroutines, each of which:

  1. Waits for a job_id on the channel.
  2. Atomically claims the job (incoming/ → processing/) via queue_manager.
  3. Appends a 'processing_started' lifecycle event to the job metadata.
  4. Fires a process-started HTTP callback to Poneglyph (best-effort).
  5. Calls pipeline.process_job() — the core OCR/LLM/webhook pipeline.
  6. Records callback outcome in metadata (callback_delivered / callback_attempts).
  7. Moves the job to completed/ or retries/fails it via queue_manager.

The QueueWatcher feeds the channel from filesystem events.
HTTP handlers feed it indirectly: they call write_job_to_queue() which
writes to incoming/, and the watcher picks it up.

The internal asyncio.Queue acts as a backpressure buffer — if all workers
are busy, new events accumulate in the channel up to `worker_queue_size`.
If the channel is full, the watcher logs a warning but the job is safe in
incoming/ and will be picked up on the next restart's crash-recovery scan.

Backward Compatibility
----------------------
`enqueue_job_id(job_id)` and `enqueue(item)` are preserved.  Callers that
previously pushed directly into the in-memory queue now transparently write
to the filesystem queue instead.  The behaviour from the caller's perspective
is identical.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from app import queue_manager
from app.pipeline import process_document, process_job
from app.webhook import deliver_processing_started_callback

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.worker")


# ---------------------------------------------------------------------------
# Legacy job envelope (backward compatibility)
# ---------------------------------------------------------------------------

@dataclass
class Job:
    """
    Legacy direct-submission job.

    Used by tests and any callers that enqueue a document object directly
    rather than going through the filesystem queue.  The worker routes
    these straight to process_document() (the old in-memory path).
    """
    document_id: int
    file_content: bytes
    filename: str


class Worker:
    """Filesystem-backed async worker pool."""

    def __init__(self, settings: "Settings") -> None:
        self._settings = settings
        # Internal channel — fed by QueueWatcher (and by enqueue_job_id for compat)
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
    # Enqueue helpers (backward-compatible public API)
    # ------------------------------------------------------------------

    async def enqueue_job_id(self, job_id: uuid.UUID, source: str = "http_v2") -> None:
        """
        Publish a job onto the filesystem queue.

        This writes the job to data/queue/incoming/ atomically.
        The QueueWatcher will detect the new directory and push the
        job_id onto the internal channel.

        Raises asyncio.QueueFull (re-raised as RuntimeError with friendly
        message) if both the filesystem write succeeded but the internal
        channel is temporarily at capacity — callers should return HTTP 503.
        """
        queue_manager.write_job_to_queue(
            job_id=job_id,
            source=source,
            settings=self._settings,
        )
        logger.info("Job %s written to queue/incoming/ (source=%s)", job_id, source)

    async def enqueue_job_id_with_meta(
        self,
        job_id: uuid.UUID,
        source: str,
        original_filename: str,
        poneglyph_file_path: str,
        callback_url: Optional[str] = None,
    ) -> None:
        """
        Publish a job with full lifecycle metadata onto the filesystem queue.

        Used by the HTTP handlers (V1 and V2) to pass filename and
        Poneglyph file reference for the lifecycle audit trail.
        """
        queue_manager.write_job_to_queue(
            job_id=job_id,
            source=source,
            settings=self._settings,
            original_filename=original_filename,
            poneglyph_file_path=poneglyph_file_path,
            callback_url=callback_url,
        )
        logger.info(
            "Job %s written to queue/incoming/ (source=%s, file=%s)",
            job_id, source, original_filename,
        )

    async def enqueue(self, item) -> None:
        """Backward-compatible enqueue alias (used by legacy callers)."""
        if isinstance(item, uuid.UUID):
            await self.enqueue_job_id(item)
        elif hasattr(item, "id") and isinstance(item.id, uuid.UUID):
            await self.enqueue_job_id(item.id)
        else:
            # Legacy direct job objects with document_id — push straight onto
            # the internal channel so the old code path still works
            self._queue.put_nowait(item)

    def push_to_channel(self, job_id: str) -> None:
        """
        Push a job_id string directly onto the internal asyncio channel.

        Called by QueueWatcher (from the event loop thread via call_soon_threadsafe)
        and by crash-recovery code that moves stranded jobs back to incoming/.
        """
        try:
            self._queue.put_nowait(job_id)
        except asyncio.QueueFull:
            logger.warning(
                "Internal channel full — job %s will be recovered on next restart "
                "(it remains in queue/incoming/)",
                job_id,
            )

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    async def _consume(self, worker_id: int) -> None:
        """Long-lived worker coroutine.  Processes one job at a time."""
        logger.info("Worker %d started", worker_id)
        while True:
            try:
                item = await self._queue.get()

                if isinstance(item, str):
                    # Normal path: filesystem-backed job
                    await self._process_fs_job(item, worker_id)

                elif isinstance(item, uuid.UUID):
                    # Should not normally reach here, but handle defensively
                    await self._process_fs_job(str(item), worker_id)

                elif hasattr(item, "document_id"):
                    # Legacy direct job object (kept for backward compat)
                    logger.info(
                        "Worker %d: processing legacy document %d", worker_id, item.document_id
                    )
                    try:
                        await process_document(
                            document_id=item.document_id,
                            file_content=item.file_content,
                            filename=item.filename,
                            settings=self._settings,
                        )
                    except Exception:
                        logger.exception(
                            "Worker %d: unhandled error on legacy document", worker_id
                        )

                self._queue.task_done()

            except asyncio.CancelledError:
                logger.info("Worker %d cancelled", worker_id)
                raise
            except Exception:
                logger.exception("Worker %d: unexpected error in consumer loop", worker_id)
                await asyncio.sleep(1)  # prevent tight spin on persistent errors

    async def _process_fs_job(self, job_id: str, worker_id: int) -> None:
        """Claim, process, and settle a single filesystem-backed job."""
        queue_dir = self._settings.queue_dir

        # ── 1. Atomically claim the job — only one worker wins ──────────────
        if not queue_manager.claim_job(job_id, queue_dir):
            return  # another worker already claimed it

        logger.info("Worker %d processing job %s", worker_id, job_id)

        # ── 2. Load metadata (for callback_url and attempt count) ────────────
        meta = queue_manager.load_job_meta(job_id, queue_dir)
        attempt = (meta.attempt if meta else 0) + 1

        # ── 3. Record processing_started lifecycle event ─────────────────────
        queue_manager.append_lifecycle_event(
            job_id, queue_dir, "processing_started",
            detail=f"Worker {worker_id}, attempt {attempt}",
        )

        # ── 4. Fire process-started callback to Poneglyph (best-effort) ──────
        try:
            started_ok = await deliver_processing_started_callback(
                job_id=job_id,
                attempt=attempt,
                settings=self._settings,
            )
            if started_ok:
                queue_manager.append_lifecycle_event(
                    job_id, queue_dir, "callback_sent",
                    detail="process-started callback delivered",
                )
            else:
                queue_manager.append_lifecycle_event(
                    job_id, queue_dir, "callback_failed",
                    detail="process-started callback failed (processing continues)",
                )
        except Exception:
            logger.exception("Worker %d: exception during process-started callback", worker_id)

        # ── 5. Run OCR → LLM → DB → completion webhook ──────────────────────
        try:
            job_uuid = uuid.UUID(job_id)
            result = await process_job(
                job_id=job_uuid,
                settings=self._settings,
            )

            # process_job returns (all_success, callback_ok) when webhook_url set,
            # or just bool for backwards compat with the legacy V1 path.
            if isinstance(result, tuple):
                _all_success, callback_ok = result
            else:
                callback_ok = True  # V1/no-webhook path — no completion callback to track

            # ── 6. Record completion callback outcome in metadata ────────────
            if callback_ok:
                queue_manager.mark_callback_delivered(job_id, queue_dir)
            else:
                queue_manager.increment_callback_attempts(
                    job_id, queue_dir, error="Completion callback delivery failed"
                )

            # ── 7. Move job to completed/ ────────────────────────────────────
            queue_manager.complete_job(job_id, queue_dir)
            logger.info("Worker %d completed job %s", worker_id, job_id)

        except Exception as exc:
            error_msg = str(exc)
            logger.exception("Worker %d failed on job %s", worker_id, job_id)
            was_retried = queue_manager.retry_or_fail_job(
                job_id=job_id,
                queue_dir=queue_dir,
                error=error_msg,
                max_attempts=self._settings.queue_max_attempts,
            )
            if was_retried:
                logger.info("Job %s re-queued for retry", job_id)
