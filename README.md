# Great Sage

Great Sage is a self-hosted Document Intelligence Engine. It was designed to run locally alongside Poneglyph (DocuNest) to handle the heavy lifting of document ingestion, optical character recognition (OCR), and structured data extraction via Large Language Models. By running locally, it ensures that no sensitive data is ever sent to external cloud providers.

## Architecture

Great Sage is built as a modern Python backend using FastAPI. It operates asynchronously to prevent blocking the main application during resource-intensive operations like model inference and text extraction.

### Core Components
- **Framework**: FastAPI (Python 3.10+)
- **Database**: SQLite with SQLModel for object-relational mapping
- **OCR Engine**: PyMuPDF for native PDFs, with a Tesseract OCR fallback for images and scanned documents
- **LLM Integration**: Ollama, defaulting to the qwen2.5 model
- **Async Execution**: asyncio background queues for task processing

### Request Lifecycle

The system decouples API ingestion from processing through a strictly defined lifecycle:

1. **Ingestion**: A client or service submits a document via a POST request.
2. **Immediate Acknowledgment**: Great Sage immediately returns a 202 Accepted response and enqueues the job. This prevents the caller from waiting during the potentially long LLM inference step.
3. **Background Processing**: A background worker picks up the job. It first runs the OCR process to extract text, then sends that text along with any context to the local Ollama instance for classification and data extraction.
4. **Data Persistence**: The structured JSON results, such as document type, names, and ID numbers, are saved to the local SQLite database.
5. **Webhook Delivery**: Finally, if a webhook URL was provided, Great Sage securely POSTs the results back to the caller, signing the payload with a shared secret to ensure authenticity.

### Internal Architecture

The codebase is structured to enforce a strict separation of concerns:

- **`app/main.py`**: The application entry point. Wires the FastAPI routes, database lifecycle, and starts background workers.
- **`app/pipeline.py`**: Orchestrates the core processing sequence (OCR -> LLM -> Database -> Webhook) and gracefully handles file-level errors.
- **`app/ocr.py`**: Handles text extraction using PyMuPDF for native documents and Tesseract OCR for images.
- **`app/llm.py`**: The intelligence layer that communicates with the local Ollama instance, constructs prompts, and parses the structured JSON output.
- **`app/webhook.py`**: Manages the asynchronous delivery of signed webhook payloads back to the caller.
- **`app/worker.py`**: Implements the asyncio queue consumer that fetches jobs and runs the pipeline without blocking the main API.
- **`app/routers/`**: Contains the API route definitions for job management and processing.
- **`data/`**: Runtime storage directory for the SQLite database and temporary job files.

## API Reference

The API is separated into Batch Processing (V2) and Legacy (V1) endpoints.

### Batch Processing API (V2)

**Submit a Job**
`POST /api/v2/jobs`
Uploads one or more files to start an asynchronous batch job.
- **Headers**: X-API-Key or Authorization: Bearer <key>
- **Content-Type**: multipart/form-data
- **Payload**:
  - files: One or more documents (.pdf, .png, .jpg, .jpeg)
  - context (optional): Text instructions for the LLM
  - webhook_url (optional): Target URL for completion notification

**Get Job Status and Results**
`GET /api/v2/jobs/{job_id}`
Fetches the current status and extracted metadata for a given job.

**Cancel a Job**
`PUT /api/v2/jobs/{job_id}/cancel`
Cancels a queued or active job. The worker will stop processing subsequent files in the batch.

**Delete a Job**
`DELETE /api/v2/jobs/{job_id}`
Deletes the job record and removes associated uploaded files from disk.

### Legacy API (V1) and Health

**Submit a Single Document**
`POST /api/v1/analyze`
Accepts a single file and a document ID. The job is processed and dispatches a webhook callback when finished.

**Health Check**
`GET /health`
Verifies that the service, Tesseract OCR, and Ollama are reachable and healthy.

## Configuration

Configuration values are loaded from a .env file. Key environment variables include:

- GREAT_SAGE_API_KEY: API key required to authenticate inbound requests.
- PONEGLYPH_WEBHOOK_URL: Destination webhook URL for legacy V1 callbacks.
- PONEGLYPH_WEBHOOK_SECRET: Shared secret used to sign outbound webhook payloads.
- OLLAMA_URL: Base URL for the local Ollama instance.
- OLLAMA_MODEL: Ollama model name for document classification.
- OLLAMA_TIMEOUT_SECONDS: Timeout in seconds for LLM inference requests.
- MAX_UPLOAD_BYTES: Maximum allowed file upload size.
- WORKER_QUEUE_SIZE: Maximum number of concurrent jobs in the memory queue.

## Getting Started

### Prerequisites
- Python 3.10 or higher
- Tesseract OCR installed and accessible in your system path
- Ollama running with the designated model pulled locally

### Setup Instructions

1. Clone the repository and create a virtual environment:
   ```powershell
   cd great-sage
   python -m venv venv
   .\venv\Scripts\Activate
   ```

2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

3. Configure your environment by copying .env.example to .env and updating the values.

4. Run the application:
   ```powershell
   uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
   ```

### Testing

Run the automated test suite with pytest:
```powershell
pytest -v
```
