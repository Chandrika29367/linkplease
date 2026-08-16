from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import DMJob, BlockedAttempt
from app.schemas import StatsResponse

router = APIRouter()

@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    # sent: confirmed delivered DMs
    sent = db.query(DMJob).filter(DMJob.status == "delivered").count()
    
    # failed: permanently failed DMs
    failed = db.query(DMJob).filter(DMJob.status == "failed").count()
    
    # queued: jobs waiting to send, retry, or reconcile (queued, sending, accepted)
    queued = db.query(DMJob).filter(DMJob.status.in_(["queued", "sending", "accepted"])).count()
    
    # duplicates_blocked: actual duplicate DM decisions prevented
    duplicates_blocked = db.query(BlockedAttempt).count()
    
    return StatsResponse(
        sent=sent,
        failed=failed,
        queued=queued,
        duplicates_blocked=duplicates_blocked
    )
