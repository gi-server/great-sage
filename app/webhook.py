"""
Webhook delivery to Poneglyph.

After processing completes (success or failure), the result is POSTed
to the configured endpoint with the shared webhook secret.

Two additional lifecycle callbacks are sent:
  - process-started: fired immediately after a worker claims a job
  - Completion callback failure is tracked in queue metadata; the
    `deliver_job_webhook` return value drives that tracking.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

import httpx

from app.schemas import JobWebhookPayload, WebhookPayload

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.webhook")

WEBHOOK_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
INITIAL_BACKOFF = 2


async def _post_with_retries(url: str, body: str, settings: "Settings", log_subject: str) -> bool:
    """
    POST a JSON body to a URL with exponential-backoff retries.

    Shared by all webhook delivery paths. Returns True on a 2xx response,
    False after exhausting all retries.
    """
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": settings.poneglyph_webhook_secret,
    }

    backoff = INITIAL_BACKOFF
    async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT_SECONDS) as client:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await client.post(url, content=body, headers=headers)

                if 200 <= response.status_code < 300:
                    logger.info("Webhook delivered for %s", log_subject)
                    return True

                logger.error(
                    "Webhook delivery failed for %s — HTTP %d: %s",
                    log_subject,
                    response.status_code,
                    response.text[:500],
                )

            except httpx.TimeoutException:
                logger.error("Webhook delivery timed out for %s", log_subject)
            except Exception as exc:
                logger.error("Error delivering webhook for %s: %s", log_subject, exc)

            if attempt < MAX_RETRIES:
                logger.warning(
                    "Retrying webhook for %s in %ds (attempt %d/%d)",
                    log_subject, backoff, attempt, MAX_RETRIES,
                )
                await asyncio.sleep(backoff)
                backoff *= 2

    logger.error("Gave up delivering webhook for %s after %d attempts", log_subject, MAX_RETRIES)
    return False


async def deliver_webhook(payload: WebhookPayload, settings: "Settings") -> bool:
    """POST the V1 processing result to Poneglyph's webhook endpoint."""
    url = settings.poneglyph_webhook_url
    if not url:
        logger.error("PONEGLYPH_WEBHOOK_URL is not configured — cannot deliver webhook")
        return False

    return await _post_with_retries(
        url=url,
        body=payload.model_dump_json(),
        settings=settings,
        log_subject=f"document {payload.document_id} (status={payload.status})",
    )


async def deliver_job_webhook(payload: JobWebhookPayload, settings: "Settings", custom_url: str = None) -> bool:
    """
    POST the V2 batch job result.
    
    If custom_url is provided (e.g. from the job DB record), it uses that exact URL.
    Otherwise, it defaults to the V2 endpoint derived from PONEGLYPH_WEBHOOK_URL.
    """
    if custom_url:
        url = custom_url
    else:
        base_url = settings.poneglyph_webhook_url
        if not base_url:
            logger.error("PONEGLYPH_WEBHOOK_URL is not configured — cannot deliver webhook")
            return False

        # Derive the V2 jobs endpoint from the configured V1 URL.
        # e.g. http://host/api/internal/webhook/analyze → .../webhook/jobs
        if base_url.endswith("/analyze"):
            url = base_url[: -len("/analyze")] + "/jobs"
        else:
            url = base_url

    return await _post_with_retries(
        url=url,
        body=payload.model_dump_json(),
        settings=settings,
        log_subject=f"job {payload.job_id} (status={payload.status})",
    )


async def deliver_processing_started_callback(
    job_id: str,
    attempt: int,
    settings: "Settings",
) -> bool:
    """
    Notify Poneglyph that Great Sage has claimed and is starting to process a job.

    This is a best-effort fire-and-forget signal — processing continues
    regardless of whether this callback succeeds.

    POSTs to /api/internal/webhook/job-status (derived from PONEGLYPH_WEBHOOK_URL).
    Payload:
        { "job_id": "...", "status": "processing", "attempt": N }
    """
    base_url = settings.poneglyph_webhook_url
    if not base_url:
        logger.warning(
            "PONEGLYPH_WEBHOOK_URL not configured — skipping processing-started callback for job %s",
            job_id,
        )
        return False

    # Derive the job-status endpoint.
    # .../webhook/analyze → .../webhook/job-status
    # .../webhook/jobs    → .../webhook/job-status
    # .../webhook/...     → .../webhook/job-status (general suffix replacement)
    import re
    url = re.sub(r"/webhook/[^/]+$", "/webhook/job-status", base_url)
    if url == base_url:
        # Fallback: just append /job-status to base
        url = base_url.rstrip("/") + "/job-status"

    body = _json_dumps({"job_id": job_id, "status": "processing", "attempt": attempt})

    ok = await _post_with_retries(
        url=url,
        body=body,
        settings=settings,
        log_subject=f"job {job_id} (processing-started, attempt={attempt})",
    )
    if ok:
        logger.info("Processing-started callback delivered for job %s", job_id)
    else:
        logger.warning(
            "Processing-started callback failed for job %s — continuing with processing", job_id
        )
    return ok


def _json_dumps(data: Dict[str, Any]) -> str:
    """Serialize dict to JSON string."""
    import json
    return json.dumps(data)
