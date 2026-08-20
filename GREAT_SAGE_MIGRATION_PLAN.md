# Great Sage Migration Plan

This document outlines the migration plan to separate the AI and Document Intelligence layer from the core **Poneglyph** application into a new service called **Great Sage**.

## 1. Responsibilities & Code Identification

The following code currently exists in Poneglyph but belongs to the Great Sage domain.

### A. Python OCR Microservice
**File:** `ocr_service/main.py`
**Functions:** `process_document`, `preprocess_image`
- **Current Responsibility:** Provides an HTTP endpoint to ingest files, performs PyMuPDF native extraction, applies PIL grayscale/sharpening, and executes Tesseract OCR.
- **Target Repository:** Great Sage
- **Reason:** This is the core low-level intelligence capability for text extraction. Poneglyph should not manage computer vision or OCR processes.

### B. AI Pipeline & Orchestration
**File:** `internal/handlers/upload.go`
**Function:** `processOCR(docID int, filePath string)`
- **Current Responsibility:** Orchestrates the HTTP call to the OCR service, limits OCR text size, and updates the Poneglyph DB with the `identifying` status.
- **Target Repository:** Great Sage (The orchestration logic)
- **Reason:** In the new architecture, Poneglyph will just say "process this file". The multi-step orchestration (File -> OCR -> Text -> LLM) is an intelligence pipeline that Great Sage should own entirely.

**File:** `internal/handlers/upload.go`
**Function:** `classifyDocumentWithAI(docID int, ocrText string)`
- **Current Responsibility:** Constructs the exact LLM prompt, truncates text to avoid injection, communicates with Ollama (`qwen2.5`), parses the JSON output, and applies business logic to save AI results to the DB.
- **Target Repository:** Great Sage
- **Reason:** Prompt engineering, LLM integration, and parsing fuzzy/unstructured AI output are strictly intelligence concerns.

**File:** `internal/handlers/upload.go`
**Function:** `sanitizeAIString(s string, maxLen int) string`
- **Current Responsibility:** Cleans up null bytes and ensures string bounds from LLM output.
- **Target Repository:** Great Sage
- **Reason:** Great Sage is responsible for ensuring its output strictly adheres to the promised JSON schema and doesn't contain bad bytes.

---

## 2. Boundary Design

### 1. What Poneglyph sends to Great Sage
Poneglyph will send a multi-part form data request containing the raw document file (PDF, PNG, JPG) along with context parameters.
**Payload:**
- `file`: The binary document file
- `document_id`: The Poneglyph ID for correlation
- `webhook_url`: Poneglyph's endpoint for Great Sage to push results to when finished

### 2. What Great Sage returns (via Webhook)
Great Sage will encapsulate both the OCR step and the AI classification step, returning a final unified JSON payload to Poneglyph.
**Webhook Payload:**
```json
{
  "document_id": 123,
  "status": "success",
  "ocr_text": "Extracted raw text for debugging/review...",
  "classification": {
    "document_type": "Income Tax Assessment Order",
    "person_name": "Jane Doe",
    "dob": "1990-05-15",
    "document_id_number": "ABCDE1234F"
  }
}
```

### 3. Great Sage API Endpoints
- `POST /api/v1/analyze`: Initiates processing. Returns `202 Accepted` to acknowledge receipt.
- `GET /health`: Healthcheck endpoint for infrastructure monitoring (verifies Tesseract and Ollama are reachable).

### 4. Authentication
- **Service-to-Service Token**: Both applications will share a secret key via environment variables (e.g., `GREAT_SAGE_API_KEY`).
- Poneglyph will attach this in an `Authorization: Bearer <TOKEN>` or `X-API-Key` header when calling Great Sage.
- Great Sage will use a `PONEGLYPH_WEBHOOK_SECRET` when pushing results back to Poneglyph's webhook to ensure Poneglyph only accepts results from authorized workers.

### 5. Error Handling
- **Synchronous Errors**: Great Sage returns standard HTTP 4xx for bad files (415 Unsupported Media Type) or missing auth (401 Unauthorized) when Poneglyph posts to `/analyze`.
- **Asynchronous Errors**: If Ollama crashes or OCR fails, Great Sage hits Poneglyph's webhook with `status: "failed"` and an `error_message`.
- **Graceful Degradation**: If OCR succeeds but the LLM fails, Great Sage should still return the `ocr_text` in the webhook with an empty `classification` block, allowing human reviewers to classify it manually without losing the extracted text.

### 6. Processing Status
Poneglyph's document statuses will be simplified:
- `uploaded`: File saved to disk.
- `processing`: Sent to Great Sage (consolidates the old `reading` and `identifying` states).
- `needs_review`: Received successful webhook from Great Sage.
- `failed`: Received failed webhook from Great Sage.
- `completed`: Human review finished.

### 7. Synchronous vs Asynchronous Processing
**Asynchronous** is strictly required. 
- Local Ollama inference can take 10-45+ seconds depending on hardware. 
- Blocking a Poneglyph goroutine or an HTTP connection for that long creates a severe bottleneck. 
- Great Sage must consume the file, return a fast `202 Accepted`, process the OCR and AI in a background worker, and fire a Webhook back to Poneglyph when complete.

---

## 3. Recommended Execution Order

1. **Setup Great Sage**: Create the new repository. Port `ocr_service/main.py` over to a new FastAPI/Python app.
2. **Port Ollama Logic**: Move the prompt generation and Ollama HTTP request logic into the Python app (using `httpx` or similar).
3. **Build the Asynchronous Pipeline**: Implement Celery/Redis or simple `asyncio` background tasks in Great Sage to handle the File -> OCR -> LLM -> Webhook flow.
4. **Update Poneglyph**: Strip out `processOCR` and `classifyDocumentWithAI`. Replace them with a single HTTP POST to Great Sage and a new webhook receiver endpoint (`POST /api/internal/webhook/analyze`).
