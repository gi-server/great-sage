"""
Async background worker for document processing.

Uses an asyncio.Queue to decouple request acceptance (HTTP 202) from
the actual processing pipeline. A single consumer loop runs as a
long-lived asyncio task.

This is deliberately simple — no Redis/Celery dependency. For production
scale-out, the queue abstraction can be swapped for a distributed broker.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.pipeline import process_document

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.worker")


@dataclass
class Job:
    """A unit of work placed on the processing queue."""
    document_id: int
    file_content: bytes
    filename: str


class Worker:
    """Async task queue backed by asyncio.Queue."""

    def __init__(self, settings: "Settings") -> None:
        self._settings = settings
        self._queue: asyncio.Queue[Job] = asyncio.Queue(
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

    async def enqueue(self, job: Job) -> None:
        """
        Add a job to the queue.

        Raises asyncio.QueueFull if the queue is at capacity
        (callers should return HTTP 503).
        """
        self._queue.put_nowait(job)
        logger.info(
            "Enqueued document %d (%d bytes, queue_size=%d)",
            job.document_id,
            len(job.file_content),
            self._queue.qsize(),
        )

    async def _consume(self) -> None:
        """Long-lived consumer that processes jobs sequentially."""
        logger.info("Consumer loop started")
        while True:
            try:
                job = await self._queue.get()
                logger.info("Processing document %d", job.document_id)
                try:
                    await process_document(
                        document_id=job.document_id,
                        file_content=job.file_content,
                        filename=job.filename,
                        settings=self._settings,
                    )
                except Exception:
                    # Pipeline should never raise, but be defensive
                    logger.exception(
                        "Unhandled error processing document %d",
                        job.document_id,
                    )
                finally:
                    self._queue.task_done()
            except asyncio.CancelledError:
                logger.info("Consumer loop cancelled")
                raise
            except Exception:
                logger.exception("Unexpected error in consumer loop")
                await asyncio.sleep(1)  # Prevent tight spin on persistent errors
