import datetime
import hmac
import hashlib
import json
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.database import get_db
from app.config import settings
from app.models import Event
from app.schemas import WebhookPayload
from anyio.from_thread import run

router = APIRouter()

def verify_signature_or_raise(headers, body: bytes):
    """
    Verifies that the incoming request has a valid X-PseudoGram-Signature header.
    Rejects the request with HTTP 401 if the signature is invalid.
    """
    signature_header = headers.get("X-PseudoGram-Signature")
    if not signature_header or not signature_header.startswith("sha256="):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid X-PseudoGram-Signature header"
        )
    
    expected_signature = signature_header.split("sha256=")[1]
    
    secret = settings.PSEUDOGRAM_API_KEY.encode("utf-8")
    h = hmac.new(secret, body, hashlib.sha256)
    computed_signature = h.hexdigest()
    
    if not hmac.compare_digest(computed_signature, expected_signature):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Signature verification failed"
        )

@router.post("/webhook")
def webhook_endpoint(request: Request, db: Session = Depends(get_db)):
    # 1. Read body synchronously from the worker thread
    body = run(request.body)
    
    # 2. Verify signature
    verify_signature_or_raise(request.headers, body)
    
    # 3. Parse payload
    try:
        payload_dict = json.loads(body)
        payload = WebhookPayload(**payload_dict)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON payload structure: {str(e)}"
        )
    
    event_id = payload.event_id
    event_type = payload.event_type
    data = payload.data
    comment_id = data.comment_id
    user_id = data.from_.user_id if data.from_ else None
    text = data.text

    # 4. Ingest and persist raw event immediately
    try:
        # Check event_id duplicate first (fast SELECT)
        event_exists = db.query(Event).filter(Event.event_id == event_id).first()
        if event_exists:
            return {"status": "ignored", "detail": "Duplicate event_id"}
        
        # Save event to be processed asynchronously by the background worker
        db_event = Event(
            event_id=event_id,
            event_type=event_type,
            comment_id=comment_id,
            user_id=user_id,
            text=text,
            received_at=datetime.datetime.utcnow(),
            processed_at=None
        )
        db.add(db_event)
        db.commit()
        return {"status": "ok"}
        
    except IntegrityError:
        db.rollback()
        return {"status": "ignored", "detail": "Duplicate event_id"}
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
