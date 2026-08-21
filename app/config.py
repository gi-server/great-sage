"""
Centralized configuration loaded from environment variables.

All secrets and service URLs are configured here — nothing is hardcoded.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

# Load environment variables from .env file (if present) before any config is read
load_dotenv()

@dataclass(frozen=True)
class Settings:
    """Immutable application settings loaded once at startup."""

    # --- Authentication ---
    great_sage_api_key: str = field(default="")
    poneglyph_webhook_secret: str = field(default="")

    # --- Poneglyph webhook ---
    poneglyph_webhook_url: str = field(default="")

    # --- Ollama ---
    ollama_url: str = field(default="http://127.0.0.1:11434")
    ollama_model: str = field(default="qwen2.5")
    ollama_timeout_seconds: int = field(default=180)

    # --- Processing limits ---
    max_upload_bytes: int = field(default=15 * 1024 * 1024)  # 15 MB
    max_ocr_text_chars: int = field(default=50_000)
    max_llm_input_chars: int = field(default=3_000)
    max_ai_string_length: int = field(default=255)

    # --- Worker ---
    worker_queue_size: int = field(default=64)

    def validate(self) -> list[str]:
        """Return a list of configuration problems (empty = OK)."""
        problems: list[str] = []
        if not self.great_sage_api_key:
            problems.append("GREAT_SAGE_API_KEY is not set")
        if not self.poneglyph_webhook_url:
            problems.append("PONEGLYPH_WEBHOOK_URL is not set")
        if not self.poneglyph_webhook_secret:
            problems.append("PONEGLYPH_WEBHOOK_SECRET is not set")
        return problems


def load_settings() -> Settings:
    """Build a Settings instance from the current environment."""
    return Settings(
        great_sage_api_key=os.getenv("GREAT_SAGE_API_KEY", ""),
        poneglyph_webhook_secret=os.getenv("PONEGLYPH_WEBHOOK_SECRET", ""),
        poneglyph_webhook_url=os.getenv("PONEGLYPH_WEBHOOK_URL", ""),
        ollama_url=os.getenv("OLLAMA_URL", "http://127.0.0.1:11434"),
        ollama_model=os.getenv("OLLAMA_MODEL", "qwen2.5"),
        ollama_timeout_seconds=int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180")),
        max_upload_bytes=int(os.getenv("MAX_UPLOAD_BYTES", str(15 * 1024 * 1024))),
        max_ocr_text_chars=int(os.getenv("MAX_OCR_TEXT_CHARS", "50000")),
        max_llm_input_chars=int(os.getenv("MAX_LLM_INPUT_CHARS", "3000")),
        max_ai_string_length=int(os.getenv("MAX_AI_STRING_LENGTH", "255")),
        worker_queue_size=int(os.getenv("WORKER_QUEUE_SIZE", "64")),
    )
