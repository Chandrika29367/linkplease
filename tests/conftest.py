import os
import sys
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

# Set up environment variables before imports
os.environ["PSEUDOGRAM_API_KEY"] = "test-secret-key-1234"
os.environ["DATABASE_URL"] = "sqlite:///./test_linkplease.db"

from app.database import Base, get_db
from app.main import app

# Create test database engine
engine = create_engine(
    os.environ["DATABASE_URL"],
    connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)
    if os.path.exists("./test_linkplease.db"):
        try:
            os.remove("./test_linkplease.db")
        except Exception:
            pass

@pytest.fixture(autouse=True)
def clean_db():
    db = TestingSessionLocal()
    try:
        # Delete from all tables in dependency order
        for table in reversed(Base.metadata.sorted_tables):
            db.execute(table.delete())
        db.commit()
    finally:
        db.close()

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

@pytest.fixture
def db_session():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

class MockPseudoGramClient:
    def __init__(self):
        self.send_dm_mock = None
        self.get_dm_status_mock = None
        self.send_dm_calls = []
        self.get_dm_status_calls = []

    async def send_dm(self, recipient_user_id: str, message: str, comment_id: str, idempotency_key: str):
        self.send_dm_calls.append({
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id,
            "idempotency_key": idempotency_key
        })
        if self.send_dm_mock:
            return await self.send_dm_mock(recipient_user_id, message, comment_id, idempotency_key)
        return "mock_dm_id", "queued"

    async def get_dm_status(self, dm_id: str):
        self.get_dm_status_calls.append(dm_id)
        if self.get_dm_status_mock:
            return await self.get_dm_status_mock(dm_id)
        return "delivered"

@pytest.fixture
def mock_pg_client(monkeypatch):
    from app.worker import worker_instance
    mock_client = MockPseudoGramClient()
    monkeypatch.setattr(worker_instance, "client", mock_client)
    # Clear rate limiter states
    worker_instance.post_attempts = []
    worker_instance.rate_limit_blocked_until = 0.0
    return mock_client
