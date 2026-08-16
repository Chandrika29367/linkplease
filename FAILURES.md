# LinkPlease System Limitations & Failure Modes

This document details known limitations and failure scenarios for the LinkPlease tech intern backend. The system has been designed with robust idempotency and crash recovery mechanisms, but certain operational boundaries and edge cases remain.

---

## 1. Crash Window between PseudoGram POST Succeeded and DB Commit
* **Scenario**: Our background worker fetches a queued job, updates its state to `sending`, and sends the `POST /v1/dm/send` request. The PseudoGram API accepts the request, schedules the DM, and returns a `202` response. However, our application process crashes (or loses database connectivity) immediately after receiving the response but *before* it can write the returned `dm_id` and update the state to `accepted` in SQLite.
* **Impact**: On process recovery, the startup routine resets the job's status from `sending` back to `queued`.
* **Resolution & Limit**: The worker will attempt to process the job again. Because the `idempotency_key` (e.g. `job_<uuid>_attempt_1`) was generated and persisted in the database *prior* to the first request, the retry uses the exact same key. PseudoGram's API recognizes the duplicate key and returns the original `dm_id` rather than sending a second DM.
* **Failure Condition**: If PseudoGram's own internal storage evicts or fails to remember the idempotency key (e.g., due to an API restart or cache expiration on their side), our retry will be treated as a new request, resulting in a duplicate DM being sent to the user.

---

## 2. Delayed Deletion Webhook (Deletion after Delivery)
* **Scenario**: A user makes a matching comment, and the system enqueues and quickly delivers the DM. Afterwards, the user deletes their comment. Due to network latency or queue delays on the PseudoGram webhook delivery side, the `comment.deleted` webhook arrives seconds or minutes after the DM was already delivered.
* **Impact**: The system receives the deletion webhook, searches for the job, and finds its status is already `delivered`.
* **Limit**: Because the DM is already in the recipient's inbox and PseudoGram does not provide an "unsend/delete DM" API endpoint, the system cannot undo the delivery. It logs that the DM cannot be recalled, which is a physical API limitation.

---

## 3. SQLite Concurrency under Horizontal Scaling
* **Scenario**: The system is scaled horizontally by running multiple application container replicas (e.g. on AWS ECS or Kubernetes) behind a load balancer.
* **Impact**: SQLite is a file-based database. It requires local file system locking to serialize writes.
* **Limit**: If replicas are deployed, they cannot share a single SQLite database file safely without a network filesystem (like NFS or AWS EFS). If they do share it via NFS, SQLite's locking mechanism will experience severe file-system latency, database corruption, or frequent "database is locked" errors. If they do not share it, their databases will drift, violating the unique user/rule constraint.
* **Resolution**: To support true horizontal scaling, the database layer must be migrated to a client-server RDBMS like PostgreSQL, and the worker rate limiter must be coordinated via a distributed lock manager (like Redis/Redlock).

---

## 4. Extreme Backlog Latency due to Strict API Rate Limiting
* **Scenario**: A viral post attracts 1,200 comments matching the keyword `PRICE` within a few minutes. 1,200 jobs are successfully ingested and queued in the database.
* **Impact**: The background worker is bound by PseudoGram's strict rate limit of 10 POST requests per rolling 60 seconds (~1 request every 6 seconds on average).
* **Limit**: The worker must sleep between requests to avoid `429 rate_limited` blocks. At this rate, it will take:
  $$\text{Latency} = 1,200 \times 6\text{ seconds} = 7,200\text{ seconds} = 2\text{ hours}$$
  to deliver the last DM.
* **Consequence**: The last commenter will experience a delay of up to two hours before receiving their DM. If the commenter deletes their comment during this window, the system will successfully cancel the pending job. However, if they leave the comment, the delay is unavoidable due to the external API's rate limits.
