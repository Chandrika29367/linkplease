import datetime
import pytest
from app.models import DMJob, ProcessedComment
from app.worker import worker_instance
from app.services.dm_sender import (
    InternalErrorException,
    RateLimitException,
    InvalidRequestException
)

def setup_test_job(db_session, comment_id="cmt_test") -> DMJob:
    job = DMJob(
        rule_id="rule_1",
        user_id="usr_test",
        comment_id=comment_id,
        message="Hello",
        status="queued",
        attempts=1,
        idempotency_key="initial-key-1",
        created_at=datetime.datetime.utcnow(),
        updated_at=datetime.datetime.utcnow()
    )
    db_session.add(job)
    
    processed = ProcessedComment(
        rule_id="rule_1",
        user_id="usr_test",
        comment_id=comment_id,
        status="queued",
        created_at=datetime.datetime.utcnow()
    )
    db_session.add(processed)
    db_session.commit()
    return job

@pytest.mark.asyncio
async def test_http_500_retry(db_session, mock_pg_client):
    # Setup job
    job = setup_test_job(db_session)
    
    # Configure mock to raise 500 Internal error
    async def mock_send(recipient_user_id, message, comment_id, idempotency_key):
        raise InternalErrorException("500 internal error")
    mock_pg_client.send_dm_mock = mock_send

    # Process sending
    await worker_instance._process_sending()

    # Verify job goes back to queued with same attempt and same key, next_retry_at set
    db_session.refresh(job)
    assert job.status == "queued"
    assert job.attempts == 1
    assert job.idempotency_key == "initial-key-1"
    assert job.next_retry_at is not None

@pytest.mark.asyncio
async def test_http_429_retry_after(db_session, mock_pg_client):
    job = setup_test_job(db_session)
    
    # Configure mock to raise 429 with Retry-After 15 seconds
    async def mock_send(recipient_user_id, message, comment_id, idempotency_key):
        raise RateLimitException(retry_after=15)
    mock_pg_client.send_dm_mock = mock_send

    # Process sending
    await worker_instance._process_sending()

    # Verify job is queued, next_retry_at is in ~15s, rate limiter blocks until is set
    db_session.refresh(job)
    assert job.status == "queued"
    assert job.idempotency_key == "initial-key-1"
    assert job.next_retry_at is not None
    
    now_dt = datetime.datetime.utcnow()
    diff = (job.next_retry_at - now_dt).total_seconds()
    assert 10 < diff <= 15
    
    assert worker_instance.rate_limit_blocked_until > 0.0

@pytest.mark.asyncio
async def test_http_400_no_retry(db_session, mock_pg_client):
    job = setup_test_job(db_session)
    
    # Configure mock to raise 400 Invalid request
    async def mock_send(recipient_user_id, message, comment_id, idempotency_key):
        raise InvalidRequestException("Bad request detail")
    mock_pg_client.send_dm_mock = mock_send

    # Process sending
    await worker_instance._process_sending()

    # Verify job goes to failed immediately
    db_session.refresh(job)
    assert job.status == "failed"
    
    processed = db_session.query(ProcessedComment).filter(ProcessedComment.comment_id == job.comment_id).first()
    assert processed.status == "failed"

@pytest.mark.asyncio
async def test_reconciliation_delivered(db_session, mock_pg_client):
    # Setup job in accepted state
    job = setup_test_job(db_session)
    job.status = "accepted"
    job.dm_id = "dm_ok"
    db_session.commit()

    # Configure GET poll to return delivered
    async def mock_get(dm_id):
        return "delivered"
    mock_pg_client.get_dm_status_mock = mock_get

    # Process reconciliation
    await worker_instance._process_reconciliation()

    # Verify job status changes to delivered
    db_session.refresh(job)
    assert job.status == "delivered"
    
    processed = db_session.query(ProcessedComment).filter(ProcessedComment.comment_id == job.comment_id).first()
    assert processed.status == "delivered"

@pytest.mark.asyncio
async def test_reconciliation_failed_retry(db_session, mock_pg_client):
    # Setup job in accepted state, attempts=1
    job = setup_test_job(db_session)
    job.status = "accepted"
    job.dm_id = "dm_failed"
    job.attempts = 1
    db_session.commit()

    # Configure GET poll to return failed
    async def mock_get(dm_id):
        return "failed"
    mock_pg_client.get_dm_status_mock = mock_get

    # Process reconciliation
    await worker_instance._process_reconciliation()

    # Verify job goes back to queued, attempts is incremented, and new idempotency_key is generated
    db_session.refresh(job)
    assert job.status == "queued"
    assert job.attempts == 2
    assert job.dm_id is None
    assert job.idempotency_key != "initial-key-1"
    assert "attempt_2" in job.idempotency_key

@pytest.mark.asyncio
async def test_reconciliation_failed_terminal(db_session, mock_pg_client):
    # Setup job in accepted state, attempts=3 (max)
    job = setup_test_job(db_session)
    job.status = "accepted"
    job.dm_id = "dm_failed"
    job.attempts = 3
    db_session.commit()

    # Configure GET poll to return failed
    async def mock_get(dm_id):
        return "failed"
    mock_pg_client.get_dm_status_mock = mock_get

    # Process reconciliation
    await worker_instance._process_reconciliation()

    # Verify job status goes to failed permanently
    db_session.refresh(job)
    assert job.status == "failed"
    assert job.attempts == 3
    
    processed = db_session.query(ProcessedComment).filter(ProcessedComment.comment_id == job.comment_id).first()
    assert processed.status == "failed"
