"""
Authentication helpers for service-to-service communication.

- Inbound:  Validates that Poneglyph's request carries the correct API key.
- Outbound: Signs the webhook callback with the shared webhook secret.
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import Settings

# Accept the key in either header — flexible for different client conventions.
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_bearer_header = APIKeyHeader(name="Authorization", auto_error=False)


def verify_api_key(
    settings: Settings,
    x_api_key: str | None = Security(_api_key_header),
    authorization: str | None = Security(_bearer_header),
) -> str:
    """
    Dependency that extracts and validates the API key.

    Accepts:
      - X-API-Key: <key>
      - Authorization: Bearer <key>
    """
    token: str | None = None

    if x_api_key:
        token = x_api_key
    elif authorization:
        parts = authorization.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]

    if not token or token != settings.great_sage_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return token
