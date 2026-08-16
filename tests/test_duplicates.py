import asyncio
import json
import pytest
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.models import DMJob, BlockedAttempt, ProcessedComment
from app.routes.webhook import webhook_endpoint
from tests.conftest import TestingSessionLocal
import hmac
import hashlib

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

def test_duplicate_event_id(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})

    payload = {
        "event_id": "evt_dup",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_dup",
            "post_id": "post_1",
            "text": "PRICE list",
            "from": {"user_id": "usr_dup", "username": "user.dup"}
        }
    }

    # First delivery
    res1 = send_webhook(client, payload)
    assert res1.status_code == 200
    assert res1.json()["status"] == "ok"

    # Second delivery of same event_id
    res2 = send_webhook(client, payload)
    assert res2.status_code == 200
    assert res2.json()["status"] == "ignored"
    assert "Duplicate event_id" in res2.json()["detail"]

    # Process events in background worker
    from app.worker import worker_instance
    import asyncio
    asyncio.run(worker_instance._process_events())

    # Verify only one job created
    jobs = db_session.query(DMJob).filter(DMJob.comment_id == "cmt_dup").all()
    assert len(jobs) == 1

def test_same_user_multiple_comments(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})

    # First comment
    payload1 = {
        "event_id": "evt_c1",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_c1",
            "post_id": "post_1",
            "text": "PRICE please",
            "from": {"user_id": "usr_same", "username": "user.same"}
        }
    }
    res1 = send_webhook(client, payload1)
    assert res1.status_code == 200

    # Second comment from same user for same rule (different comment_id / event_id)
    payload2 = {
        "event_id": "evt_c2",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:24.481Z",
        "data": {
            "comment_id": "cmt_c2",
            "post_id": "post_1",
            "text": "what is the price?",
            "from": {"user_id": "usr_same", "username": "user.same"}
        }
    }
    res2 = send_webhook(client, payload2)
    assert res2.status_code == 200

    # Process events in background worker
    from app.worker import worker_instance
    import asyncio
    asyncio.run(worker_instance._process_events())

    # Verify only one DM job exists for usr_same
    jobs = db_session.query(DMJob).filter(DMJob.user_id == "usr_same").all()
    assert len(jobs) == 1

    # Verify a blocked attempt is recorded
    blocked = db_session.query(BlockedAttempt).filter(BlockedAttempt.user_id == "usr_same").all()
    assert len(blocked) == 1
    assert blocked[0].comment_id == "cmt_c2"

@pytest.mark.asyncio
async def test_simultaneous_webhooks_for_same_user_rule(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})

    # Two requests with different event_ids and comment_ids but same user_id
    payload1 = {
        "event_id": "evt_sim1",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.000Z",
        "data": {
            "comment_id": "cmt_sim1",
            "post_id": "post_1",
            "text": "PRICE list",
            "from": {"user_id": "usr_sim", "username": "user.sim"}
        }
    }

    payload2 = {
        "event_id": "evt_sim2",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.001Z",
        "data": {
            "comment_id": "cmt_sim2",
            "post_id": "post_1",
            "text": "tell me PRICE",
            "from": {"user_id": "usr_sim", "username": "user.sim"}
        }
    }

    req1 = make_starlette_request(payload1)
    req2 = make_starlette_request(payload2)

    db1 = TestingSessionLocal()
    db2 = TestingSessionLocal()
    try:
        import anyio
        # Run concurrently in thread pool
        res1, res2 = await asyncio.gather(
            anyio.to_thread.run_sync(webhook_endpoint, req1, db1),
            anyio.to_thread.run_sync(webhook_endpoint, req2, db2)
        )
        
        # Process events in background
        from app.worker import worker_instance
        await worker_instance._process_events()
        
        # Verify one succeeded and the other was processed (but blocked due to unique constraint)

        assert res1 == {"status": "ok"}
        assert res2 == {"status": "ok"}
        
        # Check database using active session
        jobs = db_session.query(DMJob).filter(DMJob.user_id == "usr_sim").all()
        assert len(jobs) == 1
        
        blocked = db_session.query(BlockedAttempt).filter(BlockedAttempt.user_id == "usr_sim").all()
        assert len(blocked) == 1
        
        processed = db_session.query(ProcessedComment).filter(ProcessedComment.user_id == "usr_sim").all()
        assert len(processed) == 1

    finally:
        db1.close()
        db2.close()

def test_multiple_users_match_rule(client, db_session):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})

    for i in range(1, 4):
        payload = {
            "event_id": f"evt_u{i}",
            "event_type": "comment.created",
            "sent_at": "2026-08-10T09:14:22.481Z",
            "data": {
                "comment_id": f"cmt_u{i}",
                "text": "PRICE please",
                "from": {"user_id": f"usr_{i}", "username": f"user.{i}"}
            }
        }
        res = send_webhook(client, payload)
        assert res.status_code == 200

    # Process events in background worker
    from app.worker import worker_instance
    import asyncio
    asyncio.run(worker_instance._process_events())

    jobs = db_session.query(DMJob).all()

    assert len(jobs) == 3
    for i in range(1, 4):
        assert any(j.user_id == f"usr_{i}" for j in jobs)

def test_multiple_rules_match_comment(client, db_session):
    # Two rules
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Prices inside!"})
    client.post("/rules", json={"keyword": "CATALOG", "dm_message": "Here is the catalog"})

    payload = {
        "event_id": "evt_multi_rule",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_multi",
            "text": "Send the PRICE and CATALOG please!",
            "from": {"user_id": "usr_multi", "username": "user.multi"}
        }
    }
    res = send_webhook(client, payload)
    assert res.status_code == 200

    # Process events in background worker
    from app.worker import worker_instance
    import asyncio
    asyncio.run(worker_instance._process_events())

    # Verify two jobs are created for the same comment/user

    jobs = db_session.query(DMJob).filter(DMJob.user_id == "usr_multi").all()
    assert len(jobs) == 2
    assert any(j.message == "Prices inside!" for j in jobs)
    assert any(j.message == "Here is the catalog" for j in jobs)
