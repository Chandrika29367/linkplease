import hmac
import hashlib
import json
import pytest
from app.models import Event, DMJob, DeletedComment, ProcessedComment
from app.worker import worker_instance

def get_signature(body_bytes: bytes, secret: str) -> str:
    h = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    return f"sha256={h.hexdigest()}"

def send_webhook(client, payload: dict, secret: str = "test-secret-key-1234", custom_sig: str = None):
    body_bytes = json.dumps(payload).encode("utf-8")
    sig = custom_sig or get_signature(body_bytes, secret)
    headers = {"X-PseudoGram-Signature": sig}
    return client.post("/webhook", data=body_bytes, headers=headers)

def test_webhook_invalid_signature(client):
    payload = {
        "event_id": "evt_1",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {"comment_id": "cmt_1"}
    }
    # Send with bad key
    response = send_webhook(client, payload, secret="wrong-secret")
    assert response.status_code == 401
    
    # Send with custom bad signature format
    response = send_webhook(client, payload, custom_sig="invalid-format")
    assert response.status_code == 401

def test_webhook_valid_signature_ingestion(client, db_session):
    # Setup rule
    rule_res = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})
    rule_id = rule_res.json()["rule_id"]

    payload = {
        "event_id": "evt_1",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_1",
            "post_id": "post_1",
            "text": "Tell me the PRICE please",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_1",
                "username": "user.one"
            }
        }
    }
    
    # Send webhook
    response = send_webhook(client, payload)
    assert response.status_code == 200
    
    # Process events in background worker
    import asyncio
    asyncio.run(worker_instance._process_events())
    
    # Ensure event is persisted
    event = db_session.query(Event).filter(Event.event_id == "evt_1").first()
    assert event is not None
    assert event.comment_id == "cmt_1"
    
    # Ensure processed_comments constraint record is added
    processed = db_session.query(ProcessedComment).filter(
        ProcessedComment.rule_id == rule_id,
        ProcessedComment.user_id == "usr_1"
    ).first()
    assert processed is not None
    assert processed.status == "queued"

    # Ensure job is enqueued
    job = db_session.query(DMJob).filter(
        DMJob.rule_id == rule_id,
        DMJob.user_id == "usr_1"
    ).first()
    assert job is not None
    assert job.status == "queued"
    assert job.message == "Price list: $10"

@pytest.mark.asyncio
async def test_webhook_deletion_cancels_job(client, db_session, mock_pg_client):
    # Setup rule
    rule_res = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})
    
    # Send comment.created
    created_payload = {
        "event_id": "evt_c1",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_c1",
            "post_id": "post_1",
            "text": "PRICE tag please",
            "from": {"user_id": "usr_c1", "username": "user.c1"}
        }
    }
    res1 = send_webhook(client, created_payload)
    assert res1.status_code == 200
    
    # Process event
    await worker_instance._process_events()
    
    # Verify job is queued
    job = db_session.query(DMJob).filter(DMJob.comment_id == "cmt_c1").first()
    assert job is not None
    assert job.status == "queued"

    # Send comment.deleted before sending the DM
    deleted_payload = {
        "event_id": "evt_d1",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:25.481Z",
        "data": {"comment_id": "cmt_c1"}
    }
    res2 = send_webhook(client, deleted_payload)
    assert res2.status_code == 200
    
    # Process deletion event
    await worker_instance._process_events()
    
    # Ensure job is cancelled in db
    db_session.refresh(job)
    assert job.status == "cancelled"
    
    # Ensure processed record is cancelled
    processed = db_session.query(ProcessedComment).filter(ProcessedComment.comment_id == "cmt_c1").first()
    assert processed.status == "cancelled"

    # Now, if background worker runs, it should skip sending
    await worker_instance._process_sending()
    assert len(mock_pg_client.send_dm_calls) == 0

@pytest.mark.asyncio
async def test_out_of_order_deletion(client, db_session, mock_pg_client):
    # Setup rule
    rule_res = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list: $10"})
    
    # Send comment.deleted FIRST
    deleted_payload = {
        "event_id": "evt_d2",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:20.481Z",
        "data": {"comment_id": "cmt_o1"}
    }
    res1 = send_webhook(client, deleted_payload)
    assert res1.status_code == 200
    
    # Process deletion
    await worker_instance._process_events()
    
    # Verify deleted comment is tracked
    deleted_tracked = db_session.query(DeletedComment).filter(DeletedComment.comment_id == "cmt_o1").first()
    assert deleted_tracked is not None

    # Send comment.created LATER (but represents older event)
    created_payload = {
        "event_id": "evt_c2",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_o1",
            "post_id": "post_1",
            "text": "PRICE check",
            "from": {"user_id": "usr_o1", "username": "user.o1"}
        }
    }
    res2 = send_webhook(client, created_payload)
    assert res2.status_code == 200
    
    # Process creation
    await worker_instance._process_events()
    
    # Verify no job is created
    job = db_session.query(DMJob).filter(DMJob.comment_id == "cmt_o1").first()
    assert job is None
