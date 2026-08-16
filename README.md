# LinkPlease Tech Intern Backend

A lightweight, production-quality, and highly reliable backend for **LinkPlease**. It ingests comment webhooks from Instagram/PseudoGram, processes them asynchronously using a persistent database-backed job queue, and sends direct messages (DMs) to users who comment matching keywords.

---

## Architecture Overview

The system is built as a single-process application using **FastAPI**, **SQLite**, and **SQLAlchemy** 2.x. It does not introduce heavy distributed frameworks like Redis or Celery to keep the setup simple, easy to understand, and highly performant.

```
+-------------------------------------------------------------+
|                     FastAPI Web Server                      |
|                                                             |
|  POST /rules         POST /webhook          GET /stats      |
+----------------------------+---------------------+----------+
                             |                     |
                             v (Write)             v (Query)
                       +-----+---------------------+----+
                       |           SQLite DB            |
                       |       (WAL Mode Enabled)       |
                       +-----+---------------------+----+
                             ^                     ^
                             | (Read/Update)       | (Poll)
+----------------------------+---------------------+----------+
|                  Asyncio Background Worker                  |
|                                                             |
|   1. Rate Limiter (10req/60s)    2. GET /v1/dm/{dm_id}      |
+----------------------------+--------------------------------+
                             |
                             v (HTTP requests)
                   +---------+----------+
                   |  PseudoGram Mock   |
                   |      Server        |
                   +--------------------+
```

### Components
1. **Web App (FastAPI)**:
   - **`POST /rules`**: Allows creators to create comment-to-DM rules.
   - **`POST /webhook`**: Receives webhook payloads from PseudoGram. It performs HMAC-SHA256 signature verification, deduplicates events on `event_id`, matches comment text to rules, and enqueues jobs in a single transaction.
   - **`GET /stats`**: Returns persistent, accurate statistics dynamically calculated from database records.
2. **Background Worker (Asyncio task)**:
   - Spawns at app startup inside the FastAPI lifespan loop.
   - Polls the SQLite `dm_jobs` queue.
   - Sends DMs via `POST /v1/dm/send`, respecting the 10 requests / 60 seconds rate limit.
   - Polls accepted DMs (`GET /v1/dm/{dm_id}`) for delivery confirmation.
   - Handles network errors and `500` status codes using exponential backoff, and respects `429` using `Retry-After`.

---

## Setup & Local Run Instructions

### 1. Requirements
- Python 3.10+
- SQLite

### 2. Installation
Clone or navigate to the project directory and install the dependencies:
```bash
python -m pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the root directory (you can copy `.env.example` as a template):
```bash
PSEUDOGRAM_BASE_URL=https://pseudogram-api.onrender.com
PSEUDOGRAM_API_KEY=your_secret_api_key_here
DATABASE_URL=sqlite:///./linkplease.db
```

### 4. Running the Application Locally
Run the FastAPI application using Uvicorn:
```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
```
This automatically initializes the database tables (in WAL mode) and starts the background worker.

### 5. Running the Automated Tests
Run unit and integration tests using pytest:
```bash
python -m pytest
```

To run the high-concurrency integration load test (simulating 500 webhooks in less than 10 seconds):
```bash
python tests/load_test.py
```

---

## Key Strategies & Implementation Details

### 1. Webhook Signature Verification
Incoming webhooks from PseudoGram contain an `X-PseudoGram-Signature: sha256=<hex>` header. The webhook endpoint computes the HMAC-SHA256 of the raw request body using `PSEUDOGRAM_API_KEY` as the secret and rejects mismatched requests with HTTP `401 Unauthorized`.

### 2. Duplicate Protection & Idempotency
- **Event ID Deduplication**: Every event is persisted in the `events` table with `event_id` as the primary key. If a duplicate `event_id` arrives (e.g. redelivered webhook), the INSERT statement raises an `IntegrityError` which we catch and return `200 OK` (no-op) immediately.
- **User-Rule Idempotency**: To prevent a user from receiving a DM twice for the same rule, we enforce a database-level `UNIQUE(rule_id, user_id)` constraint on the `processed_comments` table. If concurrent webhooks arrive for the same user/rule, only one commit succeeds. The other raises an `IntegrityError`, which we catch and write to a `blocked_attempts` table for stats before returning `200 OK`.
- **Out-of-Order Deletions**: If a user comments and immediately deletes it, the `comment.deleted` event might arrive *before* `comment.created`. We record deleted comment IDs in a `deleted_comments` table. When `comment.created` arrives later, we check this table and skip queueing the job.

### 3. Rate-Limiting Strategy
- The background worker tracks the timestamps of the last 10 successful/attempted `POST /v1/dm/send` requests in a sliding 60-second window.
- If 10 requests have been sent, the worker sleeps until the oldest request slides out of the 60-second window.
- If the server receives an HTTP `429 rate_limited` response, the worker extracts the `Retry-After` header and pauses all outgoing DM requests until that duration has elapsed.

### 4. Retry & Idempotency Key Strategy
- **Transient Failures (HTTP 500 or network timeouts)**: When a send request fails due to transient network or server errors, we reuse the **exact same** `idempotency_key` (e.g., `job_<uuid>_attempt_1`) and retry. This ensures that if the server actually received the first request but crashed, the retry does not send a duplicate.
- **Confirmed Terminal Failures (GET poll returns "failed")**: If the GET status endpoint confirms the delivery failed on PseudoGram's side, we initiate a *new delivery attempt*. We increment the `attempts` count in the database, which automatically generates a **new** attempt-specific idempotency key (`job_<uuid>_attempt_2`) so that PseudoGram does not return the cached failed status.

### 5. Stats Integrity
Stats are dynamically computed by querying the persistent database state:
- **`sent`**: Count of jobs in state `'delivered'`.
- **`failed`**: Count of jobs in state `'failed'`.
- **`queued`**: Count of jobs in state `'queued'`, `'sending'`, or `'accepted'`.
- **`duplicates_blocked`**: Count of rows in the `blocked_attempts` table.
Stats are accurate, persistent, and survive process restarts.

---

## Deployment & Docker Instructions

### 1. Docker Build
To package the application inside a Docker container:
```bash
docker build -t linkplease-backend .
```

### 2. Docker Run
Run the container and pass your environment variables:
```bash
docker run -d \
  -p 8000:8000 \
  -e PSEUDOGRAM_API_KEY="your_api_key" \
  -e PSEUDOGRAM_BASE_URL="https://pseudogram-api.onrender.com" \
  linkplease-backend
```

### 3. Running the PseudoGram Simulation
To run the PseudoGram simulator and retrieve results:
1. Start the backend app and ensure it is accessible at a public URL (or use local tunneling like ngrok).
2. Register your rules via `POST /rules`.
3. Register your webhook URL (`/webhook`) with the PseudoGram simulator.
4. Trigger the simulation by calling:
   ```bash
   curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
     -H "X-API-Key: <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{"webhook_url": "<YOUR_WEBHOOK_URL>"}'
   ```
5. Check your stats on `GET /stats` and verify results against `GET /v1/simulate/{run_id}/truth`.
