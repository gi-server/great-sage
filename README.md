# Great Sage

Great Sage is an independent, self-hosted document intelligence engine built to work alongside Poneglyph (or DocuNest). It handles document ingestion, OCR, and structured metadata extraction locally without sending any sensitive data to external cloud providers.

---

## Overview

Great Sage acts as an asynchronous processing engine for your document pipelines. It accepts single documents or multi-file batches, extracts textual data via OCR, and passes the contents to a local LLM (Ollama) to extract structured fields like names, dates of birth, document types, and ID numbers.

Key features include:
- **Local-first processing**: OCR and LLM inference run entirely on your own infrastructure.
- **Batch job support**: Upload multiple related documents under a single job identifier.
- **Context-aware extraction**: Pass custom context (such as instructions or reference numbers) to help the model interpret complex or multi-document batches.
- **Persistent job tracking**: Backed by a SQLite database using SQLModel, keeping track of job lifecycle states (`in_queue`, `processing`, `completed`, `cancelled`, `failed`).
- **Disk-backed ingestion**: Uploaded files are written directly to disk to prevent high memory usage during large batch uploads.
- **Full lifecycle management**: Dedicated endpoints to check job progress, retrieve final extraction outputs, cancel ongoing jobs, or delete historical records.
- **Backward compatibility**: Fully supports legacy single-file requests (`POST /api/v1/analyze`) and delivers webhooks back to Poneglyph without modification.

---

## Architecture

Great Sage decouples API ingestion from document processing using an internal job queue and background worker.

```mermaid
graph TD
    Client[Client / Poneglyph] -->|POST /api/v2/jobs<br>(Multi-file + Context)| API[FastAPI Ingestion]
    Client -->|POST /api/v1/analyze<br>(Legacy Endpoint)| API
    API -->|Save Files & Record State| DB[(SQLite Database<br>great_sage.db)]
    API -->|Enqueue Job ID| Q[Async Worker Queue]
    Q -->|Update Status: processing| DB
    Q -->|Extract Text| OCR[PyMuPDF / Tesseract OCR]
    OCR -->|Text + Context| LLM[Ollama / qwen2.5]
    LLM -->|Structured JSON| DB
    Q -->|Update Status: completed| DB
    Q -->|Deliver Webhook (if requested)| Client
    Client -->|GET /api/v2/jobs/{id}| API
    Client -->|PUT /api/v2/jobs/{id}/cancel| API
    Client -->|DELETE /api/v2/jobs/{id}| API
```

---

## Project Structure

```
great-sage/
├── app/
│   ├── main.py           # FastAPI entry point and application lifespan
│   ├── auth.py           # API key validation and webhook signing
│   ├── config.py         # Environment-based configuration
│   ├── database.py       # SQLModel database engine and session setup
│   ├── models.py         # Database schemas (Job, JobFile)
│   ├── ocr.py            # PDF extraction and Tesseract OCR logic
│   ├── llm.py            # Ollama prompt construction and response parsing
│   ├── pipeline.py       # Document and job processing pipeline
│   ├── schemas.py        # Pydantic models for requests and responses
│   ├── worker.py         # Background worker processing job queues
│   ├── webhook.py        # Outbound webhook delivery client
│   └── routers/
│       └── jobs.py       # Job management endpoints (POST, GET, PUT, DELETE)
├── data/
│   ├── great_sage.db     # SQLite database file
│   └── jobs/             # File storage directory for uploads
├── tests/
│   ├── test_app.py       # Core unit and integration tests
│   └── test_v2_jobs.py   # Batch processing and lifecycle tests
├── requirements.txt      # Python dependencies
└── README.md
```

---

## API Reference

### 1. Batch Processing API (V2)

#### Submit a Job
`POST /api/v2/jobs`

Uploads one or more files to start an asynchronous batch job.

- **Authentication**: `X-API-Key` or `Authorization: Bearer <key>`
- **Content-Type**: `multipart/form-data`
- **Parameters**:
  - `files`: One or more files (`.pdf`, `.png`, `.jpg`, `.jpeg`).
  - `context` *(optional)*: Text instructions or reference context for the LLM.
  - `webhook_url` *(optional)*: Target URL to receive a notification upon completion.

**Response (`202 Accepted`):**
```json
{
  "id": "e2f1837c-d6b9-47bb-a982-19e4871e9821",
  "status": "in_queue",
  "context": "Customer onboarding packet",
  "created_at": "2026-08-24T18:00:00Z",
  "updated_at": "2026-08-24T18:00:00Z",
  "files": [
    {
      "id": "979ad2f4-7fef-489e-9988-bb7ea375d8aa",
      "filename": "id_card.png",
      "status": "pending",
      "ocr_text": null,
      "ai_result": null,
      "error_message": null
    }
  ]
}
```

---

#### Get Job Status & Results
`GET /api/v2/jobs/{job_id}`

Fetches the current status and extracted metadata for a given job.

**Response (`200 OK`):**
```json
{
  "id": "e2f1837c-d6b9-47bb-a982-19e4871e9821",
  "status": "completed",
  "context": "Customer onboarding packet",
  "created_at": "2026-08-24T18:00:00Z",
  "updated_at": "2026-08-24T18:00:15Z",
  "files": [
    {
      "id": "979ad2f4-7fef-489e-9988-bb7ea375d8aa",
      "filename": "id_card.png",
      "status": "completed",
      "ocr_text": "GOVERNMENT OF INDIA ...",
      "ai_result": "{\"document_type\": \"Aadhaar\", \"person_name\": \"Jane Doe\", \"dob\": \"1992-05-12\", \"document_id_number\": \"XXXX XXXX 1234\"}",
      "error_message": null
    }
  ]
}
```

---

#### Cancel a Job
`PUT /api/v2/jobs/{job_id}/cancel`

Cancels a queued or active job. The worker stops processing subsequent files in the batch.

**Response (`200 OK`):**
```json
{
  "message": "Job cancelled"
}
```

---

#### Delete a Job
`DELETE /api/v2/jobs/{job_id}`

Deletes the job record and removes associated uploaded files from disk.

**Response (`200 OK`):**
```json
{
  "message": "Job deleted"
}
```

---

### 2. Legacy API (V1) & Health

#### Submit a Single Document (Poneglyph Compatible)
`POST /api/v1/analyze`

Accepts a single file and a `document_id`. The job is processed through the database pipeline and dispatches a webhook callback to Poneglyph when finished.

**Response (`202 Accepted`):**
```json
{
  "message": "Document accepted for processing",
  "document_id": 123
}
```

#### Health Check
`GET /health`

Verifies that the service, Tesseract OCR, and Ollama are reachable.

---

## Configuration

Configuration values can be set via a `.env` file or environment variables:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `GREAT_SAGE_API_KEY` | API key required to authenticate inbound requests. | `""` |
| `PONEGLYPH_WEBHOOK_URL` | Destination webhook URL for legacy V1 callbacks. | `""` |
| `PONEGLYPH_WEBHOOK_SECRET` | Shared secret used to sign outbound webhook payloads. | `""` |
| `OLLAMA_URL` | Base URL for Ollama. | `http://127.0.0.1:11434` |
| `OLLAMA_MODEL` | Ollama model name for document classification. | `qwen2.5` |
| `OLLAMA_TIMEOUT_SECONDS` | Timeout in seconds for LLM inference requests. | `180` |
| `MAX_UPLOAD_BYTES` | Maximum allowed file upload size in bytes. | `15728640` (15 MB) |
| `MAX_OCR_TEXT_CHARS` | Maximum character length extracted from OCR. | `50000` |
| `MAX_LLM_INPUT_CHARS` | Maximum character length sent to the LLM prompt. | `3000` |
| `WORKER_QUEUE_SIZE` | Maximum number of concurrent jobs in the memory queue. | `64` |

---

## Getting Started

### Prerequisites
- Python 3.10 or higher
- Tesseract OCR (installed and accessible in your system `PATH`)
- Ollama running with the model downloaded (`ollama pull qwen2.5`)

### Setup Instructions

1. **Clone the repository and create a virtual environment:**
   ```powershell
   cd great-sage
   python -m venv venv
   .\venv\Scripts\Activate
   ```

2. **Install dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   Copy `.env.example` to `.env` and configure your API keys and service URLs.

4. **Run the application:**
   ```powershell
   uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
   ```

---

## Testing

Run the automated test suite with pytest:

```powershell
pytest -v
```
