"""
Comprehensive test suite for Great Sage.

Tests cover:
  - Application imports & FastAPI route registration
  - /health endpoint
  - Authentication (missing / invalid / valid)
  - File type validation (unsupported, empty, oversized)
  - OCR text extraction (PDF native, image)
  - Structured AI response parsing & sanitization
  - Webhook success/failure delivery
  - OCR-success / LLM-failure graceful degradation path
  - Pipeline end-to-end flow
"""

from __future__ import annotations

import asyncio
import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import status
from fastapi.testclient import TestClient

from app.config import Settings
from app.llm import build_classification_prompt, classify_document, sanitize_ai_string
from app.main import create_app
from app.ocr import is_allowed_file
from app.schemas import ClassificationResult, WebhookPayload
from app.webhook import deliver_webhook
from app.worker import Job, Worker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TEST_API_KEY = "test-secret-key-1234"
TEST_WEBHOOK_SECRET = "test-webhook-secret"
TEST_WEBHOOK_URL = "http://poneglyph.test/api/internal/webhook/analyze"


def make_settings(**overrides) -> Settings:
    defaults = dict(
        great_sage_api_key=TEST_API_KEY,
        poneglyph_webhook_secret=TEST_WEBHOOK_SECRET,
        poneglyph_webhook_url=TEST_WEBHOOK_URL,
        ollama_url="http://127.0.0.1:11434",
        ollama_model="qwen2.5",
        ollama_timeout_seconds=10,
        max_upload_bytes=1 * 1024 * 1024,  # 1 MB for tests
        max_ocr_text_chars=5000,
        max_llm_input_chars=3000,
        max_ai_string_length=255,
        worker_queue_size=4,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture()
def settings() -> Settings:
    return make_settings()


@pytest.fixture()
def app(settings: Settings):
    return create_app(settings)


@pytest.fixture()
def client(app) -> TestClient:
    with TestClient(app) as c:
        yield c


def auth_headers(key: str = TEST_API_KEY) -> dict:
    return {"X-API-Key": key}


def make_tiny_png() -> bytes:
    """Create a minimal valid 1x1 white PNG in memory."""
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (1, 1), color="white").save(buf, format="PNG")
    return buf.getvalue()


def make_tiny_pdf() -> bytes:
    """Create a minimal valid 1-page PDF with text using PyMuPDF."""
    import pymupdf as fitz
    doc = fitz.open()
    page = doc.new_page(width=200, height=200)
    page.insert_text((10, 50), "Hello Great Sage test PDF")
    pdf_bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


# ===================================================================
# 1. Application imports & route registration
# ===================================================================

class TestAppStructure:
    def test_app_imports(self):
        """All application modules import without error."""
        import app.main
        import app.config
        import app.schemas
        import app.auth
        import app.ocr
        import app.llm
        import app.webhook
        import app.pipeline
        import app.worker

    def test_routes_registered(self, app):
        routes = {getattr(r, "path", "") for r in app.routes}
        assert "/api/v1/analyze" in routes
        assert "/health" in routes

    def test_app_title(self, app):
        assert app.title == "Great Sage"


# ===================================================================
# 2. Health endpoint
# ===================================================================

class TestHealthEndpoint:
    @patch("app.main.check_tesseract", return_value=True)
    @patch("app.main.check_ollama", new_callable=AsyncMock, return_value=True)
    def test_healthy(self, mock_ollama, mock_tess, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert body["tesseract"] == "ok"
        assert body["ollama"] == "ok"

    @patch("app.main.check_tesseract", return_value=False)
    @patch("app.main.check_ollama", new_callable=AsyncMock, return_value=False)
    def test_degraded(self, mock_ollama, mock_tess, client):
        resp = client.get("/health")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "degraded"


# ===================================================================
# 3. Authentication
# ===================================================================

class TestAuthentication:
    def test_missing_api_key(self, client):
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.pdf", b"fake", "application/pdf")},
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    def test_invalid_api_key(self, client):
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.pdf", b"fake", "application/pdf")},
            headers={"X-API-Key": "wrong-key"},
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    def test_valid_api_key_via_x_api_key(self, client):
        """Valid key is accepted (file type error expected, not auth error)."""
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.txt", b"hello", "text/plain")},
            headers=auth_headers(),
        )
        # Should fail on file type, NOT auth
        assert resp.status_code == status.HTTP_415_UNSUPPORTED_MEDIA_TYPE

    def test_valid_api_key_via_bearer(self, client):
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.txt", b"hello", "text/plain")},
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        )
        assert resp.status_code == status.HTTP_415_UNSUPPORTED_MEDIA_TYPE


# ===================================================================
# 4. File type validation
# ===================================================================

class TestFileValidation:
    def test_unsupported_file_type(self, client):
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.docx", b"fake", "application/vnd.openxml")},
            headers=auth_headers(),
        )
        assert resp.status_code == status.HTTP_415_UNSUPPORTED_MEDIA_TYPE

    def test_empty_file(self, client):
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.png", b"", "image/png")},
            headers=auth_headers(),
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert "empty" in resp.json()["detail"].lower()

    def test_oversized_file(self, client):
        """File exceeding max_upload_bytes (1 MB in test settings) is rejected."""
        big_content = b"x" * (1 * 1024 * 1024 + 1)
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "1"},
            files={"file": ("test.png", big_content, "image/png")},
            headers=auth_headers(),
        )
        assert resp.status_code == status.HTTP_413_REQUEST_ENTITY_TOO_LARGE

    def test_allowed_extensions(self):
        assert is_allowed_file("document.pdf") is True
        assert is_allowed_file("photo.PNG") is True
        assert is_allowed_file("scan.jpg") is True
        assert is_allowed_file("scan.jpeg") is True
        assert is_allowed_file("notes.docx") is False
        assert is_allowed_file("script.py") is False


# ===================================================================
# 5. OCR text extraction
# ===================================================================

class TestOCR:
    def test_pdf_native_extraction(self, settings):
        """PyMuPDF extracts native text from a digital PDF."""
        from app.ocr import extract_text

        pdf_bytes = make_tiny_pdf()
        text = extract_text(pdf_bytes, "test.pdf", settings)
        assert "Hello Great Sage test PDF" in text

    def test_image_ocr(self, settings):
        """Image OCR runs without crashing (Tesseract must be installed)."""
        from app.ocr import extract_text

        png_bytes = make_tiny_png()
        # A 1x1 white pixel won't produce meaningful text, but it should not crash
        text = extract_text(png_bytes, "test.png", settings)
        assert isinstance(text, str)

    def test_ocr_text_truncation(self):
        """OCR text is truncated to the configured limit."""
        s = make_settings(max_ocr_text_chars=10)
        from app.ocr import extract_text

        pdf_bytes = make_tiny_pdf()
        text = extract_text(pdf_bytes, "test.pdf", s)
        assert len(text) <= 10


# ===================================================================
# 6. AI response parsing & sanitization
# ===================================================================

class TestLLM:
    def test_sanitize_ai_string_strips_nulls(self):
        assert sanitize_ai_string("hello\x00world", 255) == "helloworld"

    def test_sanitize_ai_string_truncates(self):
        result = sanitize_ai_string("a" * 300, 100)
        assert len(result) == 100

    def test_sanitize_ai_string_none(self):
        assert sanitize_ai_string(None, 100) is None

    def test_sanitize_ai_string_empty(self):
        assert sanitize_ai_string("", 100) is None

    def test_build_prompt_contains_ocr_text(self):
        prompt = build_classification_prompt("PAN CARD ABC123")
        assert "PAN CARD ABC123" in prompt
        assert "document_type" in prompt
        assert "untrusted data" in prompt

    @pytest.mark.asyncio
    async def test_classify_document_parses_valid_response(self, settings):
        """classify_document correctly parses a well-formed Ollama response."""
        fake_classification = {
            "document_type": "Aadhaar Card",
            "person_name": "Jane Doe",
            "dob": "1990-05-15",
            "document_id_number": "1234 5678 9012",
        }
        fake_ollama_response = {
            "response": json.dumps(fake_classification),
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = json.dumps(fake_ollama_response).encode()
        mock_response.raise_for_status = MagicMock()

        with patch("app.llm.httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await classify_document("some ocr text", settings)

        assert result.document_type == "Aadhaar Card"
        assert result.person_name == "Jane Doe"
        assert result.dob == "1990-05-15"
        assert result.document_id_number == "1234 5678 9012"

    @pytest.mark.asyncio
    async def test_classify_document_invalid_json_raises(self, settings):
        """classify_document raises when Ollama returns non-JSON."""
        fake_ollama_response = {"response": "not valid json at all"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = json.dumps(fake_ollama_response).encode()
        mock_response.raise_for_status = MagicMock()

        with patch("app.llm.httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            with pytest.raises(json.JSONDecodeError):
                await classify_document("some text", settings)


# ===================================================================
# 7. Webhook delivery
# ===================================================================

class TestWebhook:
    @pytest.mark.asyncio
    async def test_webhook_success(self, settings):
        payload = WebhookPayload(
            document_id=42,
            status="success",
            ocr_text="hello",
            classification=ClassificationResult(document_type="Invoice"),
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("app.webhook.httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.post.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await deliver_webhook(payload, settings)

        assert result is True
        mock_instance.post.assert_called_once()

        # Verify webhook secret header
        call_kwargs = mock_instance.post.call_args
        assert call_kwargs.kwargs["headers"]["X-Webhook-Secret"] == TEST_WEBHOOK_SECRET

    @pytest.mark.asyncio
    async def test_webhook_failure_http_error(self, settings):
        payload = WebhookPayload(
            document_id=42,
            status="failed",
            error_message="Something broke",
        )

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("app.webhook.httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.post.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await deliver_webhook(payload, settings)

        assert result is False

    @pytest.mark.asyncio
    async def test_webhook_no_url_configured(self):
        """Webhook delivery fails gracefully when URL is not configured."""
        s = make_settings(poneglyph_webhook_url="")
        payload = WebhookPayload(document_id=1, status="failed", error_message="test")
        result = await deliver_webhook(payload, s)
        assert result is False


# ===================================================================
# 8. OCR-success / LLM-failure degradation
# ===================================================================

class TestGracefulDegradation:
    @pytest.mark.asyncio
    async def test_ocr_ok_llm_fail_preserves_text(self, settings):
        """When OCR succeeds but LLM fails, the webhook preserves ocr_text."""
        from app.pipeline import process_document

        captured_payloads: list[WebhookPayload] = []

        async def mock_deliver(payload: WebhookPayload, _settings):
            captured_payloads.append(payload)
            return True

        with patch("app.pipeline.extract_text", return_value="OCR extracted text here"), \
             patch("app.pipeline.classify_document", side_effect=Exception("Ollama down")), \
             patch("app.pipeline.deliver_webhook", side_effect=mock_deliver):

            await process_document(
                document_id=99,
                file_content=b"fakecontent",
                filename="test.pdf",
                settings=settings,
            )

        assert len(captured_payloads) == 1
        p = captured_payloads[0]
        assert p.document_id == 99
        assert p.status == "failed"
        assert p.ocr_text == "OCR extracted text here"
        assert p.classification.document_type is None
        assert "Ollama down" in p.error_message

    @pytest.mark.asyncio
    async def test_ocr_fail_sends_failed_webhook(self, settings):
        """When OCR itself fails, webhook is sent with status=failed and no text."""
        from app.pipeline import process_document

        captured_payloads: list[WebhookPayload] = []

        async def mock_deliver(payload: WebhookPayload, _settings):
            captured_payloads.append(payload)
            return True

        with patch("app.pipeline.extract_text", side_effect=RuntimeError("Tesseract crash")), \
             patch("app.pipeline.deliver_webhook", side_effect=mock_deliver):

            await process_document(
                document_id=50,
                file_content=b"bad",
                filename="test.png",
                settings=settings,
            )

        assert len(captured_payloads) == 1
        p = captured_payloads[0]
        assert p.status == "failed"
        assert p.ocr_text is None
        assert "OCR" in p.error_message

    @pytest.mark.asyncio
    async def test_full_success_pipeline(self, settings):
        """When everything succeeds, webhook is sent with status=success."""
        from app.pipeline import process_document

        captured_payloads: list[WebhookPayload] = []
        mock_classification = ClassificationResult(
            document_type="PAN Card",
            person_name="Alice",
            dob="2000-01-01",
            document_id_number="ABCDE1234F",
        )

        async def mock_deliver(payload: WebhookPayload, _settings):
            captured_payloads.append(payload)
            return True

        with patch("app.pipeline.extract_text", return_value="Some OCR text"), \
             patch("app.pipeline.classify_document", return_value=mock_classification), \
             patch("app.pipeline.deliver_webhook", side_effect=mock_deliver):

            await process_document(
                document_id=7,
                file_content=b"pdf_content",
                filename="test.pdf",
                settings=settings,
            )

        assert len(captured_payloads) == 1
        p = captured_payloads[0]
        assert p.status == "success"
        assert p.ocr_text == "Some OCR text"
        assert p.classification.document_type == "PAN Card"
        assert p.classification.person_name == "Alice"
        assert p.error_message is None


# ===================================================================
# 9. Analyze endpoint (integration-level)
# ===================================================================

class TestAnalyzeEndpoint:
    def test_accepted_response(self, client):
        """Valid request returns 202 with document_id."""
        png_bytes = make_tiny_png()
        resp = client.post(
            "/api/v1/analyze",
            data={"document_id": "42"},
            files={"file": ("scan.png", png_bytes, "image/png")},
            headers=auth_headers(),
        )
        assert resp.status_code == status.HTTP_202_ACCEPTED
        body = resp.json()
        assert body["document_id"] == 42
        assert "accepted" in body["message"].lower()

    def test_missing_document_id(self, client):
        """Missing document_id should return 422."""
        png_bytes = make_tiny_png()
        resp = client.post(
            "/api/v1/analyze",
            files={"file": ("scan.png", png_bytes, "image/png")},
            headers=auth_headers(),
        )
        assert resp.status_code == 422


# ===================================================================
# 10. Schemas
# ===================================================================

class TestSchemas:
    def test_webhook_payload_success(self):
        p = WebhookPayload(
            document_id=1,
            status="success",
            ocr_text="hello",
            classification=ClassificationResult(document_type="Invoice"),
        )
        d = p.model_dump()
        assert d["status"] == "success"
        assert d["classification"]["document_type"] == "Invoice"

    def test_webhook_payload_failure(self):
        p = WebhookPayload(
            document_id=1,
            status="failed",
            ocr_text="partial text",
            classification=ClassificationResult(),
            error_message="LLM timed out",
        )
        d = p.model_dump()
        assert d["status"] == "failed"
        assert d["error_message"] == "LLM timed out"
        assert d["ocr_text"] == "partial text"
        assert d["classification"]["document_type"] is None

    def test_config_validation_missing_keys(self):
        s = Settings()  # All defaults — no secrets set
        problems = s.validate()
        assert len(problems) >= 3  # API key, webhook URL, webhook secret


# ===================================================================
# 11. Worker
# ===================================================================

class TestWorker:
    @pytest.mark.asyncio
    async def test_queue_full_raises(self, settings):
        s = make_settings(worker_queue_size=1)
        w = Worker(s)
        await w.start()
        try:
            # Patch process_job (the pipeline entry point) to block
            with patch("app.worker.process_job", new_callable=AsyncMock) as mock_proc:
                event = asyncio.Event()
                async def slow_process(*args, **kwargs):
                    await event.wait()
                    return True, True
                mock_proc.side_effect = slow_process

                # Fill the single-slot queue
                w.push_to_channel("dummy-id-1")
                with pytest.raises(asyncio.QueueFull):
                    w._queue.put_nowait("dummy-id-2")
                event.set()
        finally:
            await w.stop()
