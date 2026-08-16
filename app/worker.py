import asyncio
import logging
import datetime
import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.database import SessionLocal
from app.models import Rule, Event, DeletedComment, ProcessedComment, DMJob, BlockedAttempt
from app.services.matcher import matches_rule
from app.services.dm_sender import (
    PseudoGramClient,
    RateLimitException,
    InternalErrorException,
    InvalidRequestException
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

class DMWorker:
    def __init__(self):
        self.client = PseudoGramClient()
        self.post_attempts = []
        self.rate_limit_blocked_until = 0.0
        self.max_attempts = 3
        self.running = False
        self.task = None

    async def start(self):
        logger.info("Starting DM background worker...")
        self.running = True
        # Recover jobs stuck in 'sending' state due to prior process crash
        await self._recover_sending_jobs()
        self.task = asyncio.create_task(self._loop())

    async def stop(self):
        logger.info("Stopping DM background worker...")
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def _recover_sending_jobs(self):
        db: Session = SessionLocal()
        try:
            sending_jobs = db.query(DMJob).filter(DMJob.status == "sending").all()
            for job in sending_jobs:
                logger.info(f"Recovering job {job.id} from 'sending' status to 'queued' for crash safety.")
                job.status = "queued"
                job.next_retry_at = None
                job.updated_at = datetime.datetime.utcnow()
            if sending_jobs:
                db.commit()
        except Exception as e:
            logger.error(f"Error during job recovery: {e}")
        finally:
            db.close()

    async def _loop(self):
        while self.running:
            try:
                # 1. Process new webhook events (match rules, enqueue jobs)
                await self._process_events()
                
                # 2. Process sending queued jobs
                await self._process_sending()
                
                # 3. Process reconciliation of accepted jobs
                await self._process_reconciliation()
            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
            
            await asyncio.sleep(0.1)

    async def _process_events(self):
        db: Session = SessionLocal()
        try:
            # Poll for unprocessed events, ordered by arrival time
            events = db.query(Event).filter(Event.processed_at == None).order_by(Event.received_at.asc()).limit(100).all()
            for event in events:
                comment_id = event.comment_id
                user_id = event.user_id
                text = event.text
                event_type = event.event_type

                if event_type == "comment.deleted":
                    # Record deletion for out-of-order tracking
                    deleted_exists = db.query(DeletedComment).filter(DeletedComment.comment_id == comment_id).first()
                    if not deleted_exists:
                        db_deleted = DeletedComment(
                            comment_id=comment_id,
                            deleted_at=datetime.datetime.utcnow()
                        )
                        db.add(db_deleted)
                    
                    # Find active jobs to cancel
                    active_jobs = db.query(DMJob).filter(
                        DMJob.comment_id == comment_id,
                        DMJob.status.in_(["queued", "sending"])
                    ).all()
                    
                    for job in active_jobs:
                        job.status = "cancelled"
                        job.updated_at = datetime.datetime.utcnow()
                        
                        processed = db.query(ProcessedComment).filter(
                            ProcessedComment.rule_id == job.rule_id,
                            ProcessedComment.user_id == job.user_id,
                            ProcessedComment.comment_id == comment_id
                        ).first()
                        if processed:
                            processed.status = "cancelled"
                            
                elif event_type == "comment.created":
                    # Check if already deleted (out-of-order event delivery)
                    is_deleted = db.query(DeletedComment).filter(DeletedComment.comment_id == comment_id).first()
                    if is_deleted:
                        event.processed_at = datetime.datetime.utcnow()
                        db.commit()
                        continue
                    
                    if not user_id:
                        event.processed_at = datetime.datetime.utcnow()
                        db.commit()
                        continue
                        
                    # Match rules
                    rules = db.query(Rule).all()
                    matched_rules = [r for r in rules if matches_rule(text, r.keyword)]
                    
                    for rule in matched_rules:
                        # Check processed_comments first to avoid IntegrityError rollback overhead
                        processed_exists = db.query(ProcessedComment).filter(
                            ProcessedComment.rule_id == rule.id,
                            ProcessedComment.user_id == user_id
                        ).first()
                        
                        if processed_exists:
                            # Record blocked attempt
                            blocked = BlockedAttempt(
                                event_id=event.event_id,
                                rule_id=rule.id,
                                user_id=user_id,
                                comment_id=comment_id,
                                created_at=datetime.datetime.utcnow()
                            )
                            db.add(blocked)
                        else:
                            # Attempt to acquire rule_id/user_id lock
                            nested = db.begin_nested()
                            try:
                                processed = ProcessedComment(
                                    rule_id=rule.id,
                                    user_id=user_id,
                                    comment_id=comment_id,
                                    status="queued",
                                    created_at=datetime.datetime.utcnow()
                                )
                                db.add(processed)
                                db.flush() # Verify constraint before creating job
                                
                                # Create DMJob
                                job_uuid = str(uuid.uuid4())
                                job = DMJob(
                                    rule_id=rule.id,
                                    user_id=user_id,
                                    comment_id=comment_id,
                                    message=rule.dm_message,
                                    status="queued",
                                    attempts=1,
                                    idempotency_key=f"job_{job_uuid}_attempt_1",
                                    created_at=datetime.datetime.utcnow(),
                                    updated_at=datetime.datetime.utcnow()
                                )
                                db.add(job)
                                nested.commit()
                            except IntegrityError:
                                nested.rollback()
                                # Log blocked attempt on concurrent constraint conflict
                                blocked = BlockedAttempt(
                                    event_id=event.event_id,
                                    rule_id=rule.id,
                                    user_id=user_id,
                                    comment_id=comment_id,
                                    created_at=datetime.datetime.utcnow()
                                )
                                db.add(blocked)
                
                event.processed_at = datetime.datetime.utcnow()
                db.commit()
        except Exception as e:
            logger.error(f"Error in _process_events: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()

    async def _enforce_rate_limit(self):
        while True:
            now_ts = asyncio.get_event_loop().time()
            self.post_attempts = [t for t in self.post_attempts if now_ts - t < 60.0]
            if len(self.post_attempts) >= 10:
                wait_time = 60.1 - (now_ts - self.post_attempts[0])
                logger.info(f"Rate limit reached. Sleeping for {wait_time:.2f}s before sending next DM")
                await asyncio.sleep(wait_time)
            else:
                break

    async def _process_sending(self):
        now_ts = asyncio.get_event_loop().time()
        if now_ts < self.rate_limit_blocked_until:
            return

        db: Session = SessionLocal()
        try:
            now_dt = datetime.datetime.utcnow()
            jobs = db.query(DMJob).filter(
                DMJob.status == "queued",
                (DMJob.next_retry_at == None) | (DMJob.next_retry_at <= now_dt)
            ).order_by(DMJob.created_at.asc()).limit(5).all()

            for job in jobs:
                await self._enforce_rate_limit()
                
                job.status = "sending"
                job.updated_at = datetime.datetime.utcnow()
                db.commit()

                await self._send_job_dm(db, job)
        except Exception as e:
            logger.error(f"Error in _process_sending: {e}", exc_info=True)
        finally:
            db.close()

    async def _send_job_dm(self, db: Session, job: DMJob):
        now_ts = asyncio.get_event_loop().time()
        self.post_attempts.append(now_ts)

        try:
            dm_id, status = await self.client.send_dm(
                recipient_user_id=job.user_id,
                message=job.message,
                comment_id=job.comment_id,
                idempotency_key=job.idempotency_key
            )
            
            job.dm_id = dm_id
            job.status = "accepted"
            job.updated_at = datetime.datetime.utcnow()
            
            processed = db.query(ProcessedComment).filter(
                ProcessedComment.rule_id == job.rule_id,
                ProcessedComment.user_id == job.user_id,
                ProcessedComment.comment_id == job.comment_id
            ).first()
            if processed:
                processed.dm_id = dm_id
                processed.status = "accepted"
            
            db.commit()
            logger.info(f"Job {job.id} successfully accepted. dm_id: {dm_id}")

        except RateLimitException as e:
            logger.warning(f"Rate limited (429) for job {job.id}: {e}")
            db.rollback()
            
            retry_after = e.retry_after
            now_ts = asyncio.get_event_loop().time()
            self.rate_limit_blocked_until = now_ts + retry_after
            
            job.status = "queued"
            job.next_retry_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=retry_after)
            job.updated_at = datetime.datetime.utcnow()
            db.commit()

        except InternalErrorException as e:
            logger.warning(f"Internal/Transient error (500) for job {job.id}: {e}")
            db.rollback()
            
            backoff_secs = min(300, 2 ** job.attempts)
            job.status = "queued"
            job.next_retry_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=backoff_secs)
            job.updated_at = datetime.datetime.utcnow()
            db.commit()

        except InvalidRequestException as e:
            logger.error(f"Terminal error (400) for job {job.id}: {e}")
            db.rollback()
            
            job.status = "failed"
            job.updated_at = datetime.datetime.utcnow()
            
            processed = db.query(ProcessedComment).filter(
                ProcessedComment.rule_id == job.rule_id,
                ProcessedComment.user_id == job.user_id,
                ProcessedComment.comment_id == job.comment_id
            ).first()
            if processed:
                processed.status = "failed"
            db.commit()

        except Exception as e:
            logger.warning(f"Unexpected error for job {job.id}: {e}")
            db.rollback()
            backoff_secs = min(300, 2 ** job.attempts)
            job.status = "queued"
            job.next_retry_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=backoff_secs)
            job.updated_at = datetime.datetime.utcnow()
            db.commit()

    async def _process_reconciliation(self):
        db: Session = SessionLocal()
        try:
            jobs = db.query(DMJob).filter(DMJob.status == "accepted").all()
            for job in jobs:
                if job.status == "cancelled":
                    continue
                
                try:
                    status = await self.client.get_dm_status(job.dm_id)
                    
                    if status == "delivered":
                        job.status = "delivered"
                        job.updated_at = datetime.datetime.utcnow()
                        
                        processed = db.query(ProcessedComment).filter(
                            ProcessedComment.rule_id == job.rule_id,
                            ProcessedComment.user_id == job.user_id,
                            ProcessedComment.comment_id == job.comment_id
                        ).first()
                        if processed:
                            processed.status = "delivered"
                        
                        db.commit()
                        logger.info(f"Job {job.id} confirmed delivered. dm_id: {job.dm_id}")

                    elif status == "failed":
                        if job.attempts < self.max_attempts:
                            new_attempts = job.attempts + 1
                            job.attempts = new_attempts
                            job.status = "queued"
                            job.dm_id = None
                            job.next_retry_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=5)
                            
                            job_uuid = str(uuid.uuid4())
                            job.idempotency_key = f"job_{job_uuid}_attempt_{new_attempts}"
                            job.updated_at = datetime.datetime.utcnow()
                            
                            processed = db.query(ProcessedComment).filter(
                                ProcessedComment.rule_id == job.rule_id,
                                ProcessedComment.user_id == job.user_id,
                                ProcessedComment.comment_id == job.comment_id
                            ).first()
                            if processed:
                                processed.status = "queued"
                                processed.dm_id = None
                            
                            db.commit()
                            logger.info(f"Job {job.id} delivery failed. Retrying attempt {new_attempts} with key {job.idempotency_key}")
                        else:
                            job.status = "failed"
                            job.updated_at = datetime.datetime.utcnow()
                            
                            processed = db.query(ProcessedComment).filter(
                                ProcessedComment.rule_id == job.rule_id,
                                ProcessedComment.user_id == job.user_id,
                                ProcessedComment.comment_id == job.comment_id
                            ).first()
                            if processed:
                                processed.status = "failed"
                            
                            db.commit()
                            logger.warning(f"Job {job.id} permanently failed after {job.attempts} attempts.")
                    
                except RateLimitException as e:
                    logger.warning(f"Rate limited while polling job {job.id}: {e}")
                except InternalErrorException as e:
                    logger.warning(f"Internal error while polling job {job.id}: {e}")
                except InvalidRequestException as e:
                    logger.error(f"Terminal error while polling job {job.id}: {e}")
                    job.status = "failed"
                    job.updated_at = datetime.datetime.utcnow()
                    
                    processed = db.query(ProcessedComment).filter(
                        ProcessedComment.rule_id == job.rule_id,
                        ProcessedComment.user_id == job.user_id,
                        ProcessedComment.comment_id == job.comment_id
                    ).first()
                    if processed:
                        processed.status = "failed"
                    db.commit()
                except Exception as e:
                    logger.warning(f"Unexpected error while polling job {job.id}: {e}")
        except Exception as e:
            logger.error(f"Error in _process_reconciliation: {e}", exc_info=True)
        finally:
            db.close()

# Shared worker instance
worker_instance = DMWorker()
