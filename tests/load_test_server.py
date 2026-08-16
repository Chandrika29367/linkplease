import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import asyncio

# 1. Patch the rate limiter on DMWorker to disable sleep blocks during load test
from app.worker import DMWorker

async def mock_enforce_rate_limit(self):
    pass

DMWorker._enforce_rate_limit = mock_enforce_rate_limit

# 2. Patch PseudoGramClient to mock outgoing network requests locally
from app.services.dm_sender import PseudoGramClient

async def mock_send_dm(self, recipient_user_id: str, message: str, comment_id: str, idempotency_key: str):
    return "mock_dm_id_load", "queued"

async def mock_get_dm_status(self, dm_id: str):
    return "delivered"

PseudoGramClient.send_dm = mock_send_dm
PseudoGramClient.get_dm_status = mock_get_dm_status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8001, log_level="info")
