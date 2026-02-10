# Dockerfile for Polymarket Insider Detection System
# Build image for ingestion and workers

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Copy local packages referenced in pyproject.toml (needed for poetry install)
COPY queue_system/ ./queue_system/
COPY contracts/ ./contracts/

# Install Python dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY . .

# Default command (override in docker-compose)
CMD ["celery", "-A", "workers.celery_app", "worker", "--loglevel=info"]
