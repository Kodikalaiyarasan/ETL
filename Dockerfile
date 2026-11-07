# ═══════════════════════════════════════════════
# E-Commerce Data Lakehouse — Python Application Image
# ═══════════════════════════════════════════════

FROM python:3.11-slim-bookworm

LABEL maintainer="E-Commerce Data Lakehouse"
LABEL description="Python application container for data generation, warehouse loading, and ES indexing"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make scripts executable
RUN chmod +x /app/scripts/*.sh

# Default command
CMD ["python", "-u", "/app/orchestrator/pipeline.py"]

