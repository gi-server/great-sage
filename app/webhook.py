"""
Webhook delivery to Poneglyph.

After processing completes (success or failure), the result is POSTed
to the configured PONEGLYPH_WEBHOOK_URL with the shared webhook secret.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx

from app.schemas import WebhookPayload

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("great_sage.webhook")

WEBHOOK_TIMEOUT_SECONDS = 30


async def deliver_webhook(payload: WebhookPayload, settings: "Settings") -> bool:
    """
    POST the processing result to Poneglyph's webhook endpoint.

    Returns True on successful delivery (2xx), False otherwise.
    Logs but does NOT raise — webhook failures should not crash the worker.
    """
    url = settings.poneglyph_webhook_url
    if not url:
        logger.error("PONEGLYPH_WEBHOOK_URL is not configured — cannot deliver webhook")
        return False

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": settings.poneglyph_webhook_secret,
    }

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
            return False

    except httpx.TimeoutException:
        logger.error(
            "Webhook delivery timed out for document %d",
            payload.document_id,
        )
        return False
    except Exception:
        logger.exception(
            "Unexpected error delivering webhook for document %d",
            payload.document_id,
        )
        return False
