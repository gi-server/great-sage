"""
Webhook delivery to Poneglyph.

After processing completes (success or failure), the result is POSTed
to the configured endpoint with the shared webhook secret.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

from app.schemas import WebhookPayload, JobWebhookPayload

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.webhook")

WEBHOOK_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
INITIAL_BACKOFF = 2


async def _post_with_retries(url: str, body: str, settings: "Settings", log_subject: str) -> bool:
    """
    POST a JSON body to a URL with exponential-backoff retries.

    Shared by both V1 and V2 webhook delivery. Returns True on a 2xx response,
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


async def deliver_job_webhook(payload: JobWebhookPayload, settings: "Settings") -> bool:
    """
    POST the V2 batch job result to Poneglyph's /jobs webhook endpoint.

    The V2 endpoint is derived from PONEGLYPH_WEBHOOK_URL by replacing
    the trailing path segment with /jobs. Poneglyph registers both:
      POST /api/internal/webhook/analyze  (V1)
      POST /api/internal/webhook/jobs     (V2)
    """
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
