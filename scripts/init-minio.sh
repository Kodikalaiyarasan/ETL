#!/bin/bash
# ═══════════════════════════════════════════════
# E-Commerce Data Lakehouse — MinIO Initialization
# ═══════════════════════════════════════════════
# Creates the required S3 buckets in MinIO on startup.

set -e

echo "Waiting for MinIO to be ready..."
until curl -sf http://minio:9000/minio/health/live; do
    echo "  MinIO not ready yet, retrying in 2s..."
    sleep 2
done

echo "MinIO is ready. Creating buckets..."

# Install MinIO client if not present
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO client..."
    curl -sL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc
    chmod +x /usr/local/bin/mc
fi

# Configure MinIO alias
mc alias set lakehouse http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# Create buckets
mc mb --ignore-existing lakehouse/ecommerce-raw
mc mb --ignore-existing lakehouse/ecommerce-curated

echo "Buckets created successfully:"
mc ls lakehouse/

echo "MinIO initialization complete."

