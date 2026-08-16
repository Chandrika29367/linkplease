from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import engine, Base
from app.routes import rules, webhook, stats
from app.worker import worker_instance

from anyio.to_thread import current_default_thread_limiter

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Set AnyIO thread pool limit to 200 to allow high-concurrency webhook ingestion
    try:
        current_default_thread_limiter().total_tokens = 200
    except Exception:
        pass
    # Initialize SQLite database schema
    Base.metadata.create_all(bind=engine)
    # Start the asyncio background worker task
    await worker_instance.start()
    yield
    # Gracefully stop the background worker task on shutdown
    await worker_instance.stop()

app = FastAPI(
    title="LinkPlease Backend",
    description="Tech Intern Assignment for LinkPlease",
    version="1.0.0",
    lifespan=lifespan
)

# Register routes
app.include_router(rules.router)
app.include_router(webhook.router)
app.include_router(stats.router)

@app.get("/")
def read_root():
    return {"name": "LinkPlease Backend API", "status": "running"}
