from app.models import DMJob, BlockedAttempt
import datetime

def test_stats_counts(client, db_session):
    # 1. Initially all stats should be 0
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["sent"] == 0
    assert data["failed"] == 0
    assert data["queued"] == 0
    assert data["duplicates_blocked"] == 0

    # 2. Add a delivered job (should increment sent)
    job_delivered = DMJob(
        rule_id="r1", user_id="u1", comment_id="c1", message="m",
        status="delivered", attempts=1, idempotency_key="k1"
    )
    db_session.add(job_delivered)

    # 3. Add a failed job (should increment failed)
    job_failed = DMJob(
        rule_id="r1", user_id="u2", comment_id="c2", message="m",
        status="failed", attempts=3, idempotency_key="k2"
    )
    db_session.add(job_failed)

    # 4. Add queued, sending, and accepted jobs (should increment queued to 3)
    job_queued = DMJob(
        rule_id="r1", user_id="u3", comment_id="c3", message="m",
        status="queued", attempts=1, idempotency_key="k3"
    )
    job_sending = DMJob(
        rule_id="r1", user_id="u4", comment_id="c4", message="m",
        status="sending", attempts=1, idempotency_key="k4"
    )
    job_accepted = DMJob(
        rule_id="r1", user_id="u5", comment_id="c5", message="m",
        status="accepted", attempts=1, idempotency_key="k5"
    )
    db_session.add_all([job_queued, job_sending, job_accepted])

    # 5. Add cancelled job (should NOT increment sent, failed, or queued)
    job_cancelled = DMJob(
        rule_id="r1", user_id="u6", comment_id="c6", message="m",
        status="cancelled", attempts=1, idempotency_key="k6"
    )
    db_session.add(job_cancelled)

    # 6. Add blocked attempts (should increment duplicates_blocked to 2)
    blocked1 = BlockedAttempt(event_id="evt_b1", rule_id="r1", user_id="u1", comment_id="c_b1")
    blocked2 = BlockedAttempt(event_id="evt_b2", rule_id="r1", user_id="u2", comment_id="c_b2")
    db_session.add_all([blocked1, blocked2])

    db_session.commit()

    # 7. Check stats
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    
    assert data["sent"] == 1                 # only delivered
    assert data["failed"] == 1               # only failed (cancelled is not failed)
    assert data["queued"] == 3               # queued + sending + accepted
    assert data["duplicates_blocked"] == 2   # blocked attempts
