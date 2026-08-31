"""
Async background worker for document processing.

Now uses SQLModel database to track state and process multiple files per job.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING
from sqlmodel import Session
from app.database import engine
from app.models import Job, JobFile
from app.pipeline import process_job, process_document

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.worker")


class Worker:
    """Async task queue backed by asyncio.Queue for Job IDs."""

    def __init__(self, settings: "Settings") -> None:
        self._settings = settings
        self._queue: asyncio.Queue[uuid.UUID] = asyncio.Queue(
            maxsize=settings.worker_queue_size,
        )
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the consumer loop."""
        self._task = asyncio.create_task(self._consume(), name="great-sage-worker")
        logger.info("Worker started (queue capacity=%d)", self._settings.worker_queue_size)

    async def stop(self) -> None:
        """Drain the queue and cancel the consumer."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            logger.info("Worker stopped")

    async def enqueue_job_id(self, job_id: uuid.UUID) -> None:
        """
        Add a job to the queue.
        Raises asyncio.QueueFull if the queue is at capacity
        (callers should return HTTP 503).
        """
        self._queue.put_nowait(job_id)
        logger.info("Enqueued job %s (queue_size=%d)", job_id, self._queue.qsize())

    async def enqueue(self, item) -> None:
        """Backward-compatible enqueue alias."""
        if isinstance(item, uuid.UUID):
            await self.enqueue_job_id(item)
        elif hasattr(item, "id"):
            await self.enqueue_job_id(item.id)
        else:
            self._queue.put_nowait(item)

    async def _consume(self) -> None:
        """Long-lived consumer that processes jobs sequentially."""
        logger.info("Consumer loop started")
        while True:
            try:
                item = await self._queue.get()
                if isinstance(item, uuid.UUID):
                    job_id = item
                    logger.info("Processing job %s", job_id)
                    try:
                        await process_job(
                            job_id=job_id,
                            settings=self._settings,
                        )
                    except Exception:
                        logger.exception("Unhandled error processing job %s", job_id)
                elif hasattr(item, "document_id"):
                    # Legacy direct job object
                    logger.info("Processing legacy document %d", item.document_id)
                    try:
                        await process_document(
                            document_id=item.document_id,
                            file_content=item.file_content,
                            filename=item.filename,
                            settings=self._settings,
                        )
                    except Exception:
                        logger.exception("Unhandled error processing legacy document")
                self._queue.task_done()
            except asyncio.CancelledError:
                logger.info("Consumer loop cancelled")
                raise
            except Exception:
                logger.exception("Unexpected error in consumer loop")
                await asyncio.sleep(1)  # Prevent tight spin on persistent errors
