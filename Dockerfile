FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if any are needed (curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY app/ ./app/

# Expose FastAPI port
EXPOSE 8000

# Environment defaults
ENV PSEUDOGRAM_BASE_URL=https://pseudogram-api.onrender.com
ENV DATABASE_URL=sqlite:///./linkplease.db
# Key must be passed in at runtime, but we provide a default placeholder here
ENV PSEUDOGRAM_API_KEY=change-me-at-runtime

# Command to run FastAPI server with Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
