"""
Webhook delivery to Poneglyph.

After processing completes (success or failure), the result is POSTed
to the configured PONEGLYPH_WEBHOOK_URL with the shared webhook secret.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

from app.schemas import WebhookPayload

if TYPE_CHECKING:
    from app.config import Settings
    import app.schemas

logger = logging.getLogger("great_sage.webhook")

WEBHOOK_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
INITIAL_BACKOFF = 2


async def deliver_webhook(payload: WebhookPayload, settings: "Settings") -> bool:
    """
    POST the processing result to Poneglyph's webhook endpoint with retries.
    """
    url = settings.poneglyph_webhook_url
    if not url:
        logger.error("PONEGLYPH_WEBHOOK_URL is not configured — cannot deliver webhook")
        return False

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": settings.poneglyph_webhook_secret,
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    url,
                    content=payload.model_dump_json(),
                    headers=headers,
                )

            if 200 <= response.status_code < 300:
                logger.info(
                    "Webhook delivered for document %d (status=%s)",
                    payload.document_id,
                    payload.status,
                )
                return True
            else:
                logger.error(
                    "Webhook delivery failed for document %d — HTTP %d: %s",
                    payload.document_id,
                    response.status_code,
                    response.text[:500],
                )

        except httpx.TimeoutException:
            logger.error("Webhook delivery timed out for document %d", payload.document_id)
        except Exception as e:
            logger.error("Error delivering webhook for document %d: %s", payload.document_id, str(e))

        if attempt < MAX_RETRIES:
            logger.warning("Retrying webhook for document %d in %ds (Attempt %d/%d)", payload.document_id, backoff, attempt, MAX_RETRIES)
            await asyncio.sleep(backoff)
            backoff *= 2

    logger.error("Gave up delivering webhook for document %d after %d attempts", payload.document_id, MAX_RETRIES)
    return False


async def deliver_job_webhook(payload: "app.schemas.JobWebhookPayload", settings: "Settings") -> bool:
    """
    POST the V2 batch job processing result to Poneglyph's webhook endpoint with retries.
    """
    url = settings.poneglyph_webhook_url
    if not url:
        logger.error("PONEGLYPH_WEBHOOK_URL is not configured — cannot deliver webhook")
        return False

    # For V2 jobs, Poneglyph endpoint will be /api/internal/webhook/jobs
    if url.endswith("/analyze"):
        url = url.replace("/analyze", "/jobs")

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": settings.poneglyph_webhook_secret,
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    url,
                    content=payload.model_dump_json(),
                    headers=headers,
                )

            if 200 <= response.status_code < 300:
                logger.info("Webhook delivered for job %s (status=%s)", payload.job_id, payload.status)
                return True
            else:
                logger.error(
                    "Webhook delivery failed for job %s — HTTP %d: %s",
                    payload.job_id,
                    response.status_code,
                    response.text[:500],
                )

        except httpx.TimeoutException:
            logger.error("Webhook delivery timed out for job %s", payload.job_id)
        except Exception as e:
            logger.error("Error delivering webhook for job %s: %s", payload.job_id, str(e))

        if attempt < MAX_RETRIES:
            logger.warning("Retrying webhook for job %s in %ds (Attempt %d/%d)", payload.job_id, backoff, attempt, MAX_RETRIES)
            await asyncio.sleep(backoff)
            backoff *= 2

    logger.error("Gave up delivering webhook for job %s after %d attempts", payload.job_id, MAX_RETRIES)
    return False
