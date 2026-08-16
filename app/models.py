import datetime
from sqlalchemy import Column, String, Integer, DateTime, UniqueConstraint, func
from app.database import Base

class Rule(Base):
    __tablename__ = "rules"

    id = Column(String, primary_key=True, index=True)
    keyword = Column(String, nullable=False)
    dm_message = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

class Event(Base):
    __tablename__ = "events"

    event_id = Column(String, primary_key=True, index=True)
    event_type = Column(String, nullable=False)
    comment_id = Column(String, nullable=False)
    user_id = Column(String, nullable=True)
    text = Column(String, nullable=True)
    received_at = Column(DateTime, nullable=False)
    processed_at = Column(DateTime, nullable=True, index=True)

class DeletedComment(Base):
    __tablename__ = "deleted_comments"

    comment_id = Column(String, primary_key=True, index=True)
    deleted_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

class ProcessedComment(Base):
    __tablename__ = "processed_comments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    comment_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)  # 'queued', 'accepted', 'delivered', 'failed', 'cancelled'
    dm_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("rule_id", "user_id", name="uq_rule_user"),
    )

class DMJob(Base):
    __tablename__ = "dm_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    comment_id = Column(String, nullable=False, index=True)
    message = Column(String, nullable=False)
    status = Column(String, nullable=False)  # 'queued', 'sending', 'accepted', 'delivered', 'failed', 'cancelled'
    attempts = Column(Integer, default=1, nullable=False)
    dm_id = Column(String, nullable=True)
    idempotency_key = Column(String, unique=True, nullable=False, index=True)
    next_retry_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)

class BlockedAttempt(Base):
    __tablename__ = "blocked_attempts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, nullable=False)
    rule_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    comment_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
