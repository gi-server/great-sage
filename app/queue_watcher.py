"""
Filesystem watcher that bridges watchdog events into the asyncio worker queue.

watchdog runs its observer in a daemon thread that emits events synchronously.
We bridge to asyncio using `loop.call_soon_threadsafe()` so the asyncio queue
is only ever touched from the event loop thread.

Only `DirCreatedEvent` events under `queue/intake/` are acted on — each
new directory there represents a fully written, atomically placed job.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from watchdog.events import DirCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.queue_watcher")


class _IntakeHandler(FileSystemEventHandler):
    """
    watchdog event handler.

    Called from the watchdog observer thread — must not touch asyncio
    primitives directly.  Uses `loop.call_soon_threadsafe` to safely
    schedule `queue.put_nowait` on the event loop.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue) -> None:
        super().__init__()
        self._loop = loop
        self._queue = queue

    def on_created(self, event: DirCreatedEvent) -> None:
        if not isinstance(event, DirCreatedEvent):
            return

        job_id = Path(event.src_path).name

        # Validate it looks like a UUID (basic guard against OS artefacts)
        try:
            import uuid as _uuid
            _uuid.UUID(job_id)
        except ValueError:
            logger.debug("Ignoring non-UUID directory event: %s", job_id)
            return

        logger.info("Watcher detected new job in intake/: %s", job_id)

        # Thread-safe handoff to the asyncio event loop
        self._loop.call_soon_threadsafe(self._enqueue, job_id)

    def _enqueue(self, job_id: str) -> None:
        """Called on the event loop thread — safe to touch asyncio queue."""
        try:
            self._queue.put_nowait(job_id)
            logger.debug("Job %s pushed onto internal channel (qsize=%d)", job_id, self._queue.qsize())
        except asyncio.QueueFull:
            logger.error(
                "Internal channel full — dropping watcher event for job %s. "
                "The job is still in intake/ and will be recovered on restart.",
                job_id,
            )


class QueueWatcher:
    """
    Thin lifecycle wrapper around watchdog Observer + our event handler.

    Usage::

        watcher = QueueWatcher(settings, internal_queue, loop)
        watcher.start()
        ...
        watcher.stop()
    """

    def __init__(
        self,
        settings: "Settings",
        queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._settings = settings
        self._queue = queue
        self._loop = loop
        self._observer: Observer | None = None

    def start(self) -> None:
        intake_path = Path(self._settings.queue_dir) / "intake"
        intake_path.mkdir(parents=True, exist_ok=True)

        handler = _IntakeHandler(loop=self._loop, queue=self._queue)
        self._observer = Observer()
        self._observer.schedule(handler, str(intake_path), recursive=False)
        self._observer.start()
        logger.info("QueueWatcher started — watching %s", intake_path)

    def stop(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None
            logger.info("QueueWatcher stopped")
