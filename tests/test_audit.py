import asyncio
import concurrent.futures
import datetime
import hmac
import hashlib
import json
import uuid
import pytest
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.models import Event, DeletedComment, ProcessedComment, DMJob, BlockedAttempt
from app.routes.webhook import webhook_endpoint, get_db
from app.worker import worker_instance
from tests.conftest import TestingSessionLocal

def get_signature(body_bytes: bytes, secret: str) -> str:
    h = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    return f"sha256={h.hexdigest()}"

def send_webhook(client, payload: dict):
    body_bytes = json.dumps(payload).encode("utf-8")
    sig = get_signature(body_bytes, "test-secret-key-1234")
    headers = {"X-PseudoGram-Signature": sig}
    return client.post("/webhook", data=body_bytes, headers=headers)

def make_starlette_request(payload: dict) -> Request:
    body_bytes = json.dumps(payload).encode("utf-8")
    sig = get_signature(body_bytes, "test-secret-key-1234")
    headers_dict = {
        "x-pseudogram-signature": sig,
        "content-type": "application/json"
    }
    async def receive():
        return {"type": "http.request", "body": body_bytes, "more_body": False}
    scope = {
        "type": "http",
        "headers": [(k.encode("utf-8"), v.encode("utf-8")) for k, v in headers_dict.items()],
    }
    return Request(scope, receive)

# =====================================================================
# AUDIT TEST 1: 500 webhook events from 500 different users return 200
# =====================================================================
def test_audit_500_different_users(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})
    
    payloads = [
        {
            "event_id": f"evt_t1_{i}",
            "event_type": "comment.created",
            "sent_at": "2026-08-10T09:14:22Z",
            "data": {
                "comment_id": f"cmt_t1_{i}",
                "text": "PRICE please",
                "from": {"user_id": f"usr_t1_{i}", "username": f"user.{i}"}
            }
        }
        for i in range(500)
    ]
    
    # Send concurrently using thread pool to simulate concurrent web requests
    def send(payload):
        body_bytes = json.dumps(payload).encode("utf-8")
        sig = get_signature(body_bytes, "test-secret-key-1234")
        return client.post("/webhook", data=body_bytes, headers={"X-PseudoGram-Signature": sig})

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(send, payloads))
        
    for res in results:
        assert res.status_code == 200
        
    # Verify they were saved in the events table
    events_count = db_session.query(Event).count()
    assert events_count == 500

# =====================================================================
# AUDIT TEST 2: 500 webhook events from the SAME user for SAME rule
# =====================================================================
@pytest.mark.asyncio
async def test_audit_500_same_user_same_rule(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Hello"})
    
    payloads = [
        {
            "event_id": f"evt_t2_{i}",
            "event_type": "comment.created",
            "sent_at": "2026-08-10T09:14:22Z",
            "data": {
                "comment_id": f"cmt_t2_{i}",
                "text": "PRICE list",
                "from": {"user_id": "usr_t2_same", "username": "user.same"}
            }
        }
        for i in range(500)
    ]
    
    for payload in payloads:
        res = send_webhook(client, payload)
        assert res.status_code == 200
        
    # Process events in the background worker
    while db_session.query(Event).filter(Event.processed_at == None).count() > 0:
        await worker_instance._process_events()
    
    # Verify only 1 DM job is created
    jobs = db_session.query(DMJob).filter(DMJob.user_id == "usr_t2_same").all()
    assert len(jobs) == 1
    
    # Verify the remaining 499 are duplicates_blocked
    blocked = db_session.query(BlockedAttempt).filter(BlockedAttempt.user_id == "usr_t2_same").all()
    assert len(blocked) == 499

# =====================================================================
# AUDIT TEST 3: 250 users, each commenting twice for the same rule
# =====================================================================
@pytest.mark.asyncio
async def test_audit_250_users_twice(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Hello"})
    
    payloads = []
    for u in range(250):
        for c in range(2):
            payloads.append({
                "event_id": f"evt_t3_u{u}_c{c}",
                "event_type": "comment.created",
                "sent_at": "2026-08-10T09:14:22Z",
                "data": {
                    "comment_id": f"cmt_t3_u{u}_c{c}",
                    "text": "PRICE tag",
                    "from": {"user_id": f"usr_t3_{u}", "username": f"user.{u}"}
                }
            })
            
    for payload in payloads:
        res = send_webhook(client, payload)
        assert res.status_code == 200
        
    while db_session.query(Event).filter(Event.processed_at == None).count() > 0:
        await worker_instance._process_events()
    
    # Verify 250 jobs max
    jobs = db_session.query(DMJob).all()
    assert len(jobs) == 250
    
    # Verify 250 duplicates_blocked max
    blocked = db_session.query(BlockedAttempt).all()
    assert len(blocked) == 250

# =====================================================================
# AUDIT TEST 4: 500 concurrent requests, no duplicate DM jobs
# =====================================================================
@pytest.mark.asyncio
async def test_audit_500_concurrent_requests_no_duplicates(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Hello"})
    
    payloads = [
        {
            "event_id": f"evt_t4_{i}",
            "event_type": "comment.created",
            "sent_at": "2026-08-10T09:14:22Z",
            "data": {
                "comment_id": f"cmt_t4_{i}",
                "text": "PRICE please",
                "from": {"user_id": "usr_t4_sim", "username": "user.sim"}
            }
        }
        for i in range(500)
    ]
    
    requests = [make_starlette_request(p) for p in payloads]
    sessions = [TestingSessionLocal() for _ in range(500)]
    
    import anyio
    try:
        async def run_req(req, db):
            return await anyio.to_thread.run_sync(webhook_endpoint, req, db)
            
        tasks = [run_req(req, db) for req, db in zip(requests, sessions)]
        await asyncio.gather(*tasks)
        
        # Ingest in background
        while db_session.query(Event).filter(Event.processed_at == None).count() > 0:
            await worker_instance._process_events()
        
        # Verify only 1 job
        jobs = db_session.query(DMJob).filter(DMJob.user_id == "usr_t4_sim").all()
        assert len(jobs) == 1
        
        blocked = db_session.query(BlockedAttempt).filter(BlockedAttempt.user_id == "usr_t4_sim").all()
        assert len(blocked) == 499
        
    finally:
        for s in sessions:
            s.close()

# =====================================================================
# AUDIT TEST 5: comment.created followed by comment.deleted before processing
# =====================================================================
@pytest.mark.asyncio
async def test_audit_created_then_deleted_before_processing(client, db_session, mock_pg_client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Hello"})
    
    c_payload = {
        "event_id": "evt_t5_c",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22Z",
        "data": {
            "comment_id": "cmt_t5",
            "text": "PRICE check",
            "from": {"user_id": "usr_t5", "username": "user.t5"}
        }
    }
    
    d_payload = {
        "event_id": "evt_t5_d",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:24Z",
        "data": {"comment_id": "cmt_t5"}
    }
    
    send_webhook(client, c_payload)
    send_webhook(client, d_payload)
    
    # Process all events in chronological order
    await worker_instance._process_events()
    
    # Verify that any created job has been cancelled and not sent
    job = db_session.query(DMJob).filter(DMJob.comment_id == "cmt_t5").first()
    assert job is not None
    assert job.status == "cancelled"
    
    # Try sending (should skip cancelled job)
    await worker_instance._process_sending()
    assert len(mock_pg_client.send_dm_calls) == 0

# =====================================================================
# AUDIT TEST 6: comment.deleted followed by a redelivered comment.created
# =====================================================================
@pytest.mark.asyncio
async def test_audit_deleted_then_old_created_redelivered(client, db_session, mock_pg_client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Hello"})
    
    # 1. Deletion event arrives first
    d_payload = {
        "event_id": "evt_t6_d",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:20Z",
        "data": {"comment_id": "cmt_t6"}
    }
    
    # 2. Redelivered older creation event arrives second
    c_payload = {
        "event_id": "evt_t6_c",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22Z",
        "data": {
            "comment_id": "cmt_t6",
            "text": "PRICE list",
            "from": {"user_id": "usr_t6", "username": "user.t6"}
        }
    }
    
    send_webhook(client, d_payload)
    send_webhook(client, c_payload)
    
    await worker_instance._process_events()
    
    # Verify that NO active job exists (either None or marked cancelled)
    job = db_session.query(DMJob).filter(DMJob.comment_id == "cmt_t6").first()
    assert job is None or job.status == "cancelled"
    
    await worker_instance._process_sending()
    assert len(mock_pg_client.send_dm_calls) == 0

# =====================================================================
# AUDIT TEST 7: simulate process restart while jobs are queued
# =====================================================================
@pytest.mark.asyncio
async def test_audit_process_restart_queued(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Hello"})
    
    job = DMJob(
        rule_id="r_t7", user_id="u_t7", comment_id="c_t7", message="Hello",
        status="queued", attempts=1, idempotency_key="k_t7",
        created_at=datetime.datetime.utcnow(), updated_at=datetime.datetime.utcnow()
    )
    db_session.add(job)
    db_session.commit()
    
    # Simulates process restart recovery
    from app.worker import DMWorker
    new_worker = DMWorker()
    await new_worker.start()
    
    # Check that job is still in queued state and recovered successfully
    db_session.refresh(job)
    assert job.status == "queued"
    
    await new_worker.stop()

# =====================================================================
# AUDIT TEST 8: process restart after accepted but before dm_id saved
# =====================================================================
@pytest.mark.asyncio
async def test_audit_restart_uncertain_post(client, db_session):
    job = DMJob(
        rule_id="r_t8", user_id="u_t8", comment_id="c_t8", message="Hello",
        status="sending", attempts=1, idempotency_key="k_t8_attempt_1",
        created_at=datetime.datetime.utcnow(), updated_at=datetime.datetime.utcnow()
    )
    db_session.add(job)
    db_session.commit()
    
    # Restart the worker
    from app.worker import DMWorker
    new_worker = DMWorker()
    
    # Mock PseudoGram client
    from tests.conftest import MockPseudoGramClient
    mock_client = MockPseudoGramClient()
    new_worker.client = mock_client
    
    # Start recovers job stuck in 'sending' to 'queued'
    await new_worker.start()
    
    db_session.refresh(job)
    assert job.status == "queued"
    
    # Process sending
    await new_worker._process_sending()
    
    # Verify we reused the EXACT same idempotency key
    assert len(mock_client.send_dm_calls) == 1
    assert mock_client.send_dm_calls[0]["idempotency_key"] == "k_t8_attempt_1"
    
    await new_worker.stop()
