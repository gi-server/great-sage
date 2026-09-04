# Architecture Snapshot

> Generated: 2026-09-01 · Scope: **poneglyph** (DocuNest) + **great-sage** (Document Intelligence Engine)

---

## System Overview

This monorepo pair implements **DocuNest** — a private, local-first document management platform for organisations that cannot tolerate data leaving their infrastructure.  The system is split across two independent repositories that communicate exclusively through HTTP:

| Repo | Language | Role |
|---|---|---|
| **poneglyph** | Go 1.26 | Core web application — user sessions, document lifecycle, PostgreSQL, human-review workflow, static UI |
| **great-sage** | Python 3 / FastAPI | Document intelligence engine — OCR (Tesseract + PyMuPDF), AI classification (Ollama), async job queue, SQLite |

At runtime a user uploads a document through the **poneglyph** Go server; poneglyph writes the file to disk, stores a record in PostgreSQL, then calls Great Sage's REST API to submit it for processing.  Great Sage performs OCR, passes the extracted text to a local Ollama LLM, and fires a webhook back to poneglyph once classification is complete.  A human operator then reviews the AI-extracted fields before they are committed to the customer profile.

---

## 1. Directory Trees

### 1.1 `poneglyph` (DocuNest — Go API + Frontend)

```
poneglyph/
├── cmd/
│   └── api/
│       └── main.go                  # Entry point — router wiring, middleware, server start
├── internal/
│   ├── database/
│   │   └── db.go                    # PostgreSQL connection, schema init, seed admin
│   ├── handlers/
│   │   ├── admin.go                 # Admin-only: user management, DB wipe, log streaming
│   │   ├── auth.go                  # Login/logout, JWT issuance, AuthMiddleware, brute-force lockout
│   │   ├── dashboard.go             # Stats aggregation endpoint
│   │   ├── document.go              # Document listing and viewer
│   │   ├── document_user.go         # Per-customer document queries
│   │   ├── events.go                # SSE log streaming
│   │   ├── review.go                # Human-in-the-loop review confirmation
│   │   ├── share.go                 # Share-link creation/revocation/view
│   │   ├── upload.go                # File upload, MIME validation, Great Sage submission
│   │   ├── webhook.go               # Receives Great Sage callbacks, writes AI results to DB
│   │   └── webhook_test.go          # Unit tests for webhook handler
│   ├── models/
│   │   └── models.go                # Go structs: User, Customer, Document, AuditLog, DocumentShare
│   ├── services/
│   │   ├── greatsage.go             # HTTP client for submitting docs to Great Sage API
│   │   └── greatsage_test.go        # Unit tests for GreatSageClient
│   └── storage/
│       └── storage.go               # Secure file persistence (MIME-validated, crypto-UUID filenames)
├── e2e/
│   ├── tests/
│   │   ├── auth.spec.ts             # Playwright auth flow tests
│   │   └── example.spec.ts          # Playwright smoke tests
│   ├── playwright.config.ts         # Playwright config (Chromium, HTML reporter)
│   └── package.json                 # E2E dev dependencies
├── public/
│   ├── index.html                   # Single-file SPA entry (vanilla JS + CSS)
│   ├── manifest.json                # PWA manifest
│   ├── sw.js                        # Service worker (offline caching)
│   └── templates/
│       ├── admin.html               # Admin panel template
│       ├── customers.html           # Customer list/dossier template
│       ├── dashboard.html           # Dashboard / stats template
│       ├── documents.html           # Document queue template
│       ├── index.html               # Login page template
│       └── upload.html              # Upload form template
├── scratch/
│   ├── fix_compile.py               # One-off compile-error fixup script
│   ├── fix_infinite_reload.py       # Debug script for reload loop
│   ├── fix_redirect.py              # Debug script for redirect issues
│   ├── fix_syntax.py                # Syntax fixup utility
│   ├── fix_workspace.py             # Workspace repair helper
│   ├── merge.py                     # File merge utility
│   └── update_tables.py             # DB table update script
├── uploads/                         # Runtime: uploaded documents stored by server (gitignored)
├── docker-compose.yml               # PostgreSQL service definition
├── go.mod                           # Go module manifest
├── go.sum                           # Go dependency checksums
├── start.ps1                        # Windows dev-start script
├── .env.example                     # Environment variable template
└── README.md
```

### 1.2 `great-sage` (Document Intelligence Engine — Python/FastAPI)

```
great-sage/
├── app/
│   ├── __init__.py
│   ├── auth.py                      # API-key verification (X-API-Key / Bearer)
│   ├── config.py                    # Immutable Settings dataclass loaded from env
│   ├── database.py                  # SQLModel engine init, session factory, schema creation
│   ├── llm.py                       # Ollama HTTP client, document classification, AI string sanitization
│   ├── main.py                      # FastAPI app, lifespan, /api/v1/analyze, /api/v2/jobs, /health
│   ├── models.py                    # SQLModel ORM: Job, JobFile (SQLite-backed)
│   ├── ocr.py                       # Text extraction: PyMuPDF (native PDF) → Tesseract fallback
│   ├── pipeline.py                  # End-to-end processing: OCR → LLM → DB → webhook
│   ├── schemas.py                   # Pydantic request/response schemas
│   ├── webhook.py                   # Async webhook delivery to Poneglyph
│   ├── worker.py                    # asyncio.Queue-backed background worker
│   └── routers/
│       └── jobs.py                  # /api/v2/jobs CRUD (create, get, cancel, delete)
├── data/
│   └── jobs/                        # Runtime: per-job file storage (one sub-dir per UUID job)
│       └── <uuid>/                  # Each job's uploaded files live here during processing
├── tests/
│   ├── __init__.py
│   ├── test_app.py                  # pytest integration tests for API endpoints
│   └── test_v2_jobs.py              # pytest tests for v2 jobs API
├── requirements.txt                 # Python dependency manifest
├── .env.example                     # Environment variable template
└── README.md
```

---

## 2. Dependency Manifests

### 2.1 `great-sage/requirements.txt`

```text
# Great Sage — AI/Document Intelligence Service
# Core
fastapi>=0.115.0
uvicorn>=0.30.0
python-multipart>=0.0.9

# OCR & Document Processing
PyMuPDF>=1.25.0
Pillow>=10.0.0
pytesseract>=0.3.10

# HTTP client (for Ollama + webhook)
httpx>=0.27.0

# Pydantic (pulled by FastAPI, pinned for clarity)
pydantic>=2.9.0

# Testing
pytest>=8.0.0
pytest-asyncio>=0.24.0
sqlmodel>=0.0.22
```

### 2.2 `poneglyph/go.mod`

```go
module docunest

go 1.26.3

require (
	github.com/alexedwards/argon2id v1.0.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/lib/pq v1.12.3 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
```

### 2.3 `poneglyph/go.sum` (full contents)

```
github.com/alexedwards/argon2id v1.0.0 h1:wJzDx66hqWX7siL/SRUmgz3F8YMrd/nfX/xHHcQQP0w=
github.com/alexedwards/argon2id v1.0.0/go.mod h1:tYKkqIjzXvZdzPvADMWOEZ+l6+BD6CtBXMj5fnJppiw=
github.com/golang-jwt/jwt/v5 v5.3.1 h1:kYf81DTWFe7t+1VvL7eS+jKFVWaUnK9cB1qbwn63YCY=
github.com/golang-jwt/jwt/v5 v5.3.1/go.mod h1:fxCRLWMO43lRc8nhHWY6LGqRcf+1gQWArsqaEUEa5bE=
github.com/gorilla/mux v1.8.1 h1:TuBL49tXwgrFYWhqrNgrUNEY92u81SPhu7sTdzQEiWY=
github.com/gorilla/mux v1.8.1/go.mod h1:AKf9I4AEqPTmMytcMc0KkNouC66V3BtZ4qD5fmWSiMQ=
github.com/joho/godotenv v1.5.1 h1:7eLL/+HRGLY0ldzfGMeQkb7vMd0as4CfYvUVzLqw0N0=
github.com/joho/godotenv v1.5.1/go.mod h1:f4LDr5Voq0i2e/R5DDNOoa2zzDfwtkZa6DnEwAbqwq4=
github.com/lib/pq v1.12.3 h1:tTWxr2YLKwIvK90ZXEw8GP7UFHtcbTtty8zsI+YjrfQ=
github.com/lib/pq v1.12.3/go.mod h1:/p+8NSbOcwzAEI7wiMXFlgydTwcgTr3OSKMsD2BitpA=
github.com/yuin/goldmark v1.4.13/go.mod h1:6yULJ656Px+3vBD8DxQVa3kxgyrAnzto9xy5taEt/CY=
golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2/go.mod h1:djNgcEr1/C05ACkg1iLfiJU5Ep61QUkGW8qpdssI0+w=
golang.org/x/crypto v0.0.0-20210921155107-089bfa567519/go.mod h1:GvvjBRRGRdwPK5ydBHafDWAxML/pGHZbMvKqRZ5+Abc=
golang.org/x/crypto v0.14.0/go.mod h1:MVFd36DqK4CsrnJYDkBA3VC4m2GkXAM0PvzMCn4JQf4=
golang.org/x/crypto v0.55.0 h1:+KWHjbgOaAQ66dh/YlkZKHlz9ZUlq61AFirAR9ntP8M=
golang.org/x/crypto v0.55.0/go.mod h1:uq0V9dE/fzQuJtbnL+2EhWOE63vo164FY8xqEnV9xis=
golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4/go.mod h1:jJ57K6gSWd91VN4djpZkiMVwK6gcyfeH4XE8wZrZaV4=
golang.org/x/mod v0.8.0/go.mod h1:iBbtSCu2XBx23ZKBPSOrRkjjQPZFPuis4dIYUhu/chs=
golang.org/x/net v0.0.0-20190620200207-3b0461eec859/go.mod h1:z5CRVTTTmAJ677TzLLGU+0bjPO0LkuOLi4/5GtJWs/s=
golang.org/x/net v0.0.0-20210226172049-e18ecbb05110/go.mod h1:m0MpNAwzfU5UDzcl9v0D8zg8gWTRqZa9RBIspLL5mdg=
golang.org/x/net v0.0.0-20220722155237-a158d28d115b/go.mod h1:XRhObCWvk6IyKnWLug+ECip1KBveYUHfp+8e9klMJ9c=
golang.org/x/net v0.6.0/go.mod h1:2Tu9+aMcznHK/AK1HMvgo6xiTLG5rD5rZLDS+rp2Bjs=
golang.org/x/net v0.10.0/go.mod h1:0qNGK6F8kojg2nk9dLZ2mShWaEBan6FAoqfSigmmuDg=
golang.org/x/sync v0.0.0-20190423024810-112230192c58/go.mod h1:RxMgew5VJxzue5/jJTE5uejpjVlOe/izrB70Jof72aM=
golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4/go.mod h1:RxMgew5VJxzue5/jJTE5uejpjVlOe/izrB70Jof72aM=
golang.org/x/sync v0.1.0/go.mod h1:RxMgew5VJxzue5/jJTE5uejpjVlOe/izrB70Jof72aM=
golang.org/x/sys v0.0.0-20190215142949-d0b11bdaac8a/go.mod h1:STP8DvDyc/dI5b8T5hshtkjS+E42TnysNCUPdjciGhY=
golang.org/x/sys v0.0.0-20201119102817-f84b799fce68/go.mod h1:h1NjWce9XRLGQEsW7wpKNCjG9DtNlClVuFLEZdDNbEs=
golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1/go.mod h1:oPkhp1MJrh7nUepCBck5+mAzfO9JrbApNNgaTdGDITg=
golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a/go.mod h1:oPkhp1MJrh7nUepCBck5+mAzfO9JrbApNNgaTdGDITg=
golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f/go.mod h1:oPkhp1MJrh7nUepCBck5+mAzfO9JrbApNNgaTdGDITg=
golang.org/x/sys v0.5.0/go.mod h1:oPkhp1MJrh7nUepCBck5+mAzfO9JrbApNNgaTdGDITg=
golang.org/x/sys v0.8.0/go.mod h1:oPkhp1MJrh7nUepCBck5+mAzfO9JrbApNNgaTdGDITg=
golang.org/x/sys v0.13.0/go.mod h1:oPkhp1MJrh7nUepCBck5+mAzfO9JrbApNNgaTdGDITg=
golang.org/x/sys v0.47.0 h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs=
golang.org/x/sys v0.47.0/go.mod h1:4GL1E5IUh+htKOUEOaiffhrAeqysfVGipDYzABqnCmw=
golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1/go.mod h1:bj7SfCRtBDWHUb9snDiAeCFNEtKQo2Wmx5Cou7ajbmo=
golang.org/x/term v0.0.0-20210927222741-03fcf44c2211/go.mod h1:jbD1KX2456YbFQfuXm/mYQcufACuNUgVhRMnK/tPxf8=
golang.org/x/term v0.5.0/go.mod h1:jMB1sMXY+tzblOD4FWmEbocvup2/aLOaQEp7JmGp78k=
golang.org/x/term v0.8.0/go.mod h1:xPskH00ivmX89bAKVGSKKtLOWNx2+17Eiy94tnKShWo=
golang.org/x/term v0.13.0/go.mod h1:LTmsnFJwVN6bCy1rVCoS+qHT1HhALEFxKncY3WNNh4U=
golang.org/x/text v0.3.0/go.mod h1:NqM8EUOU14njkJ3fqMW+pc6Ldnwhi/IjpwHt7yyuwOQ=
golang.org/x/text v0.3.3/go.mod h1:5Zoc/QRtKVWzQhOtBMvqHzDpF6irO9z98xDceosuGiQ=
golang.org/x/text v0.3.7/go.mod h1:u+2+/6zg+i71rQMx5EYifcz6MCKuco9NR6JIITiCfzQ=
golang.org/x/text v0.7.0/go.mod h1:mrYo+phRRbMaCq/xk9113O4dZlRixOauAjOtrjsXDZ8=
golang.org/x/text v0.9.0/go.mod h1:e1OnstbJyHTd6l/uOt8jFFHp6TRDWZR/bV3emEE/zU8=
golang.org/x/text v0.13.0/go.mod h1:TvPlkZtksWOMsz7fbANvkp4WM8x/WCo/om8BMLbz+aE=
golang.org/x/tools v0.0.0-20180917221912-90fa682c2a6e/go.mod h1:n7NCudcB/nEzxVGmLbDWY5pfWTLqBcC2KZ6jyYvM4mQ=
golang.org/x/tools v0.0.0-20191119224855-298f0cb1881e/go.mod h1:b+2E5dAYhXwXZwtnZ6UAqBI28+e2cm9otk0dWdXHAEo=
golang.org/x/tools v0.1.12/go.mod h1:hNGJHUnrk76NpqgfD5Aqm5Crs+Hm0VOH/i9J2+nxYbc=
golang.org/x/tools v0.6.0/go.mod h1:Xwgl3UAJ/d3gWutnCtw505GrjyAbvKui8lOU390QaIU=
golang.org/x/xerrors v0.0.0-20190717185122-a985d3407aa7/go.mod h1:I/5z698sn9Ka8TeJc9MKroUUfqBBauWjQqLJ2OPfmY0=
```

### 2.4 `poneglyph/docker-compose.yml`

```yaml
version: '3.8'

services:
  db:
    image: postgres:15-alpine
    container_name: docunest_db
    environment:
      POSTGRES_USER: ${DB_USER:-docunest}
      POSTGRES_PASSWORD: ${DB_PASSWORD:?DB_PASSWORD must be set}
      POSTGRES_DB: ${DB_NAME:-docunest}
    ports:
      # Bind only to localhost — do not expose PostgreSQL to external interfaces
      - "127.0.0.1:5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    # Resource limits to prevent a runaway container from exhausting host memory
    deploy:
      resources:
        limits:
          memory: 512m
    restart: unless-stopped

volumes:
  pgdata:
```

### 2.5 `poneglyph/e2e/package.json`

```json
{
  "name": "e2e",
  "version": "1.0.0",
  "description": "",
  "main": "index.js",
  "scripts": {},
  "keywords": [],
  "author": "",
  "license": "ISC",
  "type": "commonjs",
  "devDependencies": {
    "@playwright/test": "^1.62.1",
    "@types/node": "^26.3.0"
  }
}
```

---

## 3. Module / Folder Summaries

### `poneglyph` (DocuNest — Go Application)

#### `cmd/api/`
The application entry point. `main.go` bootstraps the entire server: it loads `.env` via `godotenv`, calls `handlers.InitAuth()` to validate the JWT secret at startup (refusing to start with a default/empty value), connects to PostgreSQL and runs schema migrations, seeds the admin user if absent, and then wires all routes through a `gorilla/mux` router — public login and share-view routes first, then a JWT-protected subrouter for normal users, and finally an admin subrouter behind an additional role check. Static files in `./public` are served last by a catch-all file server on `:8080`.

#### `internal/database/`
Owns the global `*sql.DB` connection and all schema DDL. `ConnectDB()` constructs the PostgreSQL DSN from environment variables (with safe defaults for host/port/user), pings the database to confirm connectivity, and exposes the shared `DB` variable. `InitSchema()` executes the `CREATE TABLE IF NOT EXISTS` statements for `users`, `customers`, `documents`, `audit_logs`, and `document_shares`, and creates the necessary indexes. `SeedAdminUser()` inserts the initial admin account if the `users` table is empty.

#### `internal/handlers/`
The HTTP handler layer — ten handler files covering every route group. `auth.go` implements Argon2id login, JWT cookie issuance with short expiry, `AuthMiddleware`, IP-based brute-force lockout (10 attempts triggers a 15-minute ban), and `AdminMiddleware`. `upload.go` validates file MIME types via binary inspection, delegates persistent storage to `storage.SaveFile`, records the document in PostgreSQL, and asynchronously submits it to Great Sage via `services.GreatSageClient`. `webhook.go` receives Great Sage callbacks (`POST /api/internal/webhook/analyze` and `/jobs`), verifies the shared webhook secret header, and writes OCR and AI classification results back to the `documents` table, setting status to `needs_review`. `review.go` allows a logged-in user to confirm or correct AI-extracted fields before committing them to a customer profile. `admin.go` provides user CRUD, account disabling, password reset, and the destructive `WipeDatabase` endpoint. `share.go` issues single-use or time-limited share tokens, while `events.go` and `dashboard.go` support real-time log streaming via Server-Sent Events and dashboard stats aggregation respectively.

#### `internal/models/`
Defines the canonical Go structs — `User`, `Customer`, `Document`, `AuditLog`, `ReviewRequest`, and `DocumentShare` — with `json` field tags used across the entire application. `Document` carries all AI-extracted fields (`DocumentType`, `PersonName`, `DOB`, `DocumentIDNumber`, `Confidence`) as nullable pointers so that unprocessed records have clean zero-values without interfering with JSON serialisation.

#### `internal/services/`
Houses `GreatSageClient`, a thin HTTP client that reads `GREAT_SAGE_URL` and `GREAT_SAGE_API_KEY` from the environment and exposes `SubmitDocument(docID, filePath)` (multipart POST to `/api/v1/analyze`) and `SubmitJob(jobID, filePaths, context, webhookURL)` (multipart POST to `/api/v2/jobs`). Timeouts are set to 30 seconds for submission only — Great Sage returns `202 Accepted` immediately and delivers results asynchronously via a webhook callback.

#### `internal/storage/`
Secure file persistence layer. `SaveFile(reader, mimeType)` generates a cryptographically random 32-character hex ID, appends the canonical extension for the validated MIME type (PDF, JPEG, or PNG only — never the user-supplied filename), performs a path-traversal safety check on the resolved absolute path, and writes the file with permissions `0640`. Extension spoofing and path traversal are explicitly blocked by design and enforced at the only point where extensions are decided.

#### `e2e/`
Playwright end-to-end test suite targeting the running Go server. `auth.spec.ts` covers login and logout flows; `example.spec.ts` provides a basic smoke test. The Playwright configuration runs only Chromium in development mode (Firefox and WebKit are commented out) with HTML reporting and two retries on CI.

#### `public/`
The self-contained frontend — a vanilla-JS single-page application served directly by the Go file server. `index.html` is the main SPA shell; `templates/` holds six HTML page templates (login, dashboard, customers, documents, upload, admin) rendered client-side. A PWA `manifest.json` and `sw.js` service worker enable offline caching and browser installability.

#### `scratch/`
A collection of one-off Python utility scripts used during development and debugging (compile fixes, merge helpers, DB table updates). These are not part of the production system and are excluded from deployments.

#### `uploads/`
Runtime upload directory. All user-uploaded files are written here by `storage.SaveFile` under randomised cryptographic filenames. This directory is gitignored and is created automatically at startup by `storage.init()`.

---

### `great-sage` (Document Intelligence Engine — Python/FastAPI)

#### `app/`
The entire FastAPI application package. Exposes three route groups: `POST /api/v1/analyze` (legacy single-file endpoint, backward-compatible with poneglyph v1), the v2 jobs API (via `app/routers/jobs.py`), and `GET /health`. The `lifespan` context manager initialises the SQLite database and starts the background worker on startup, then gracefully drains the queue on shutdown.

#### `app/auth.py`
Validates inbound API keys sent by poneglyph. Accepts either an `X-API-Key` header or a `Bearer` token in `Authorization`. Returns `HTTP 401` on mismatch. API key checking is skipped entirely when `GREAT_SAGE_API_KEY` is left empty in the environment, which is useful for local development without inter-service authentication.

#### `app/config.py`
A frozen `dataclasses.dataclass` (`Settings`) that reads all configuration from environment variables at startup using `python-dotenv`. Covers the inbound API key, Ollama endpoint and model name, outbound webhook URL and secret, upload/OCR/LLM character-size limits, and worker queue capacity. Exposes a `validate()` method that returns a list of configuration errors so the application can fail-fast with a clear diagnostic message.

#### `app/database.py`
Wraps SQLModel and SQLite: creates the engine pointing at `./data/great_sage.db`, defines `init_db()` (calls `SQLModel.metadata.create_all` to provision tables), and provides a `get_session()` FastAPI dependency that yields a scoped `Session` for each request.

#### `app/llm.py`
HTTP client for a locally-running Ollama instance. `classify_document(text, context, settings)` sends a structured prompt to the configured model (default: `qwen2.5`) requesting a JSON object with `document_type`, `person_name`, `dob`, `document_id_number`, and `confidence`. `sanitize_ai_string()` strips null bytes and enforces a maximum length on every AI-returned field before it is written to the database. `check_ollama()` performs a lightweight reachability check consumed by the `/health` endpoint.

#### `app/ocr.py`
Text extraction with a dual-strategy approach: first attempts to read embedded text from PDFs using PyMuPDF (fast, lossless, requires no model inference); if the extracted text falls below a minimum threshold — indicating a scanned or image-only PDF, or an image file — it falls back to Tesseract OCR with Pillow-based image pre-processing (grayscale conversion and unsharp-mask sharpening). Platform detection at import time sets the Tesseract binary path for Windows development versus Linux production. `check_tesseract()` is used by the `/health` endpoint.

#### `app/pipeline.py`
Orchestrates the end-to-end processing flow for a single document or a full batch job: (1) extract text via `ocr.extract_text`, (2) classify via `llm.classify_document` with any job-level context string, (3) persist results to the SQLite `job_files` table, (4) update the parent `job` status to `completed` or `failed`, and (5) deliver a webhook to poneglyph if a `webhook_url` was provided. Errors at each step are caught individually so a failure in one file does not abort the rest of the batch.

#### `app/worker.py`
An `asyncio.Queue`-backed background consumer (`Worker` class). `start()` spawns a single long-running `asyncio.Task` (`_consume`) that waits for job UUIDs, fetches the full `Job` record from SQLite, and calls `pipeline.process_job`. `stop()` drains the queue gracefully and cancels the consumer task. Queue capacity is configurable via `worker_queue_size` (default 64); `enqueue_job_id()` raises immediately if the queue is full, allowing the API layer to return `503` to the caller.

#### `app/models.py`
Two SQLModel ORM tables backed by SQLite: `Job` (UUID primary key, `status`, optional `context` string, `webhook_url`, `legacy_document_id`, timestamps, and a cascading `files` relationship) and `JobFile` (UUID PK, foreign key to `Job`, `filename`, `filepath`, per-file `status`, `ocr_text`, `ai_result`, `error_message`). Job status lifecycle: `in_queue → processing → completed | failed | cancelled`.

#### `app/schemas.py`
Pydantic v2 request and response schemas used for API validation and JSON serialisation: `AcceptedResponse`, `HealthResponse`, `ClassificationResult`, `WebhookPayload`, `JobWebhookPayload`, `FileResult`, `JobResponse`, and `JobFileResponse`.

#### `app/webhook.py`
Asynchronous webhook delivery using `httpx.AsyncClient`. Signs outbound requests with the shared `PONEGLYPH_WEBHOOK_SECRET` in the `X-Webhook-Secret` header. Returns `True` on a `2xx` response and `False` on any error — webhook failures are logged but never re-raised, ensuring the worker continues processing remaining jobs regardless of delivery outcome. A 30-second timeout is applied to each delivery attempt.

#### `app/routers/jobs.py`
FastAPI `APIRouter` mounted at `/api/v2/jobs`. Implements four endpoints: `POST /` (create job, persist files to `data/jobs/<uuid>/`, enqueue for async processing, return `202`), `GET /{job_id}` (status and results poll), `PUT /{job_id}/cancel` (soft-cancel if job is not already in a terminal state), and `DELETE /{job_id}` (hard-delete the job record and remove files from disk).

#### `data/`
Runtime storage root for great-sage. `great_sage.db` (SQLite) lives at the top level; `jobs/` holds one subdirectory per job UUID, containing all uploaded files for that job during and after processing.

#### `tests/`
`pytest` suite covering the FastAPI application. `test_app.py` tests the legacy v1 `analyze` endpoint, health checks, authentication rejection, and webhook delivery callbacks. `test_v2_jobs.py` covers the v2 batch jobs API — job creation, status polling, cancellation, and deletion — using FastAPI's `TestClient`.

---

## 4. Inter-Service Communication

```
poneglyph (Go :8080)                           great-sage (Python :8000)
        |                                               |
        |  POST /api/v1/analyze  (multipart, X-API-Key)|
        |---------------------------------------------->|
        |  202 Accepted  {job_id}                       |
        |<----------------------------------------------|
        |                                               | <- asyncio worker processes
        |                                               |    OCR -> Ollama -> DB
        |  POST /api/internal/webhook/analyze           |
        |<----------------------------------------------|
        |    (X-Webhook-Secret, JSON result)            |
        |                                               |
        |  POST /api/v2/jobs  (multipart batch)         |
        |---------------------------------------------->|
        |  202 Accepted  {job_id}                       |
        |<----------------------------------------------|
        |                                               | <- processes all files
        |  POST /api/internal/webhook/jobs              |
        |<----------------------------------------------|
```

Both services share two secrets via environment variables:
- `GREAT_SAGE_API_KEY` / `X-API-Key` — poneglyph to great-sage authentication
- `PONEGLYPH_WEBHOOK_SECRET` / `X-Webhook-Secret` — great-sage to poneglyph callback authentication

---

## 5. Environment Variables Reference

### `great-sage`

| Variable | Purpose | Default |
|---|---|---|
| `GREAT_SAGE_API_KEY` | Shared API key for inbound requests from poneglyph | *(empty = auth disabled)* |
| `PONEGLYPH_WEBHOOK_URL` | URL to POST results back to poneglyph | — |
| `PONEGLYPH_WEBHOOK_SECRET` | Secret header value for signing webhook callbacks | — |
| `OLLAMA_URL` | Local Ollama base URL | `http://127.0.0.1:11434` |
| `OLLAMA_MODEL` | LLM model name | `qwen2.5` |
| `OLLAMA_TIMEOUT_SECONDS` | LLM call timeout | `180` |
| `MAX_UPLOAD_BYTES` | Per-file upload size limit | `15728640` (15 MB) |
| `WORKER_QUEUE_SIZE` | Maximum number of queued jobs | `64` |

### `poneglyph`

| Variable | Purpose | Default |
|---|---|---|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USER` | PostgreSQL user | `postgres` |
| `DB_PASSWORD` | PostgreSQL password | *(required)* |
| `DB_NAME` | PostgreSQL database name | `postgres` |
| `DB_SSLMODE` | PostgreSQL SSL mode | `disable` |
| `JWT_SECRET` | HMAC secret for JWT signing and verification | *(required — server refuses to start if missing or default)* |
| `GREAT_SAGE_URL` | Base URL of the Great Sage service | — |
| `GREAT_SAGE_API_KEY` | API key sent to Great Sage in `X-API-Key` header | — |
| `WEBHOOK_SECRET` | Secret validated on incoming Great Sage webhook callbacks | — |
| `ADMIN_USERNAME` | Username for the seeded initial admin account | — |
| `ADMIN_PASSWORD` | Password for the seeded initial admin account | — |

---

*This snapshot was generated on 2026-09-01 without modifying any source files.*
