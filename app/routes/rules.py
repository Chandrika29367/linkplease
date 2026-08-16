import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Rule
from app.schemas import RuleCreate, RuleResponse

router = APIRouter()

@router.post("/rules", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
def create_rule(rule_in: RuleCreate, db: Session = Depends(get_db)):
    rule_id = str(uuid.uuid4())
    db_rule = Rule(
        id=rule_id,
        keyword=rule_in.keyword,
        dm_message=rule_in.dm_message
    )
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return RuleResponse(
        rule_id=db_rule.id,
        keyword=db_rule.keyword,
        dm_message=db_rule.dm_message
    )
