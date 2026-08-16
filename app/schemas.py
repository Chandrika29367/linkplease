from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

# Rule schemas
class RuleCreate(BaseModel):
    keyword: str
    dm_message: str

class RuleResponse(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str

# Stats schema
class StatsResponse(BaseModel):
    sent: int
    failed: int
    queued: int
    duplicates_blocked: int

# Webhook schema
class UserFrom(BaseModel):
    user_id: str
    username: str

class WebhookData(BaseModel):
    comment_id: str
    post_id: Optional[str] = None
    text: Optional[str] = None
    created_at: Optional[str] = None
    from_: Optional[UserFrom] = Field(default=None, alias="from")

    class Config:
        populate_by_name = True

class WebhookPayload(BaseModel):
    event_id: str
    event_type: str
    sent_at: str
    data: WebhookData
