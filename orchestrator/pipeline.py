#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — Pipeline Orchestrator
====================================================
Orchestrates the end-to-end data pipeline execution:

  1. Data Generation  → Generate mock data and upload to S3 (Bronze)
  2. Spark ETL        → Bronze → Silver → Gold transformations
  3. Warehouse Load   → Load Gold data into PostgreSQL (Redshift simulation)
  4. ES Indexing      → Index reviews and clickstream into Elasticsearch

Each step is executed as a subprocess with error handling,
logging, and timing instrumentation.
"""

import logging
import os
import subprocess
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orchestrator")

# Pipeline steps configuration
PIPELINE_STEPS = [
    {
        "name": "Data Generation",
        "description": "Generate mock e-commerce data and upload to S3 (Bronze layer)",
        "command": ["python", "/app/data_generator/generate_data.py"],
        "required": True,
    },
    {
        "name": "Spark ETL Processing",
        "description": "Execute Bronze → Silver → Gold transformations using PySpark",
        "command": [
            "spark-submit",
            "--master", os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
            "--deploy-mode", "client",
            "--driver-memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"),
            "--executor-memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"),
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "--conf", "spark.hadoop.fs.s3a.endpoint=" + os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            "--conf", "spark.hadoop.fs.s3a.access.key=" + os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin"),
            "--conf", "spark.hadoop.fs.s3a.secret.key=" + os.getenv("MINIO_SECRET_KEY", "lakehouse_secret_2024"),
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            "/app/etl/spark_etl.py",
        ],
        "required": True,
    },
    {
        "name": "Warehouse Loading",
        "description": "Bulk-load Gold data into PostgreSQL analytical warehouse",
        "command": ["python", "/app/warehouse/load_warehouse.py"],
        "required": True,
    },
    {
        "name": "Elasticsearch Indexing",
        "description": "Index reviews and clickstream into Elasticsearch",
        "command": ["python", "/app/search/index_elasticsearch.py"],
        "required": False,  # Non-critical — pipeline continues if ES is down
    },
]


# ──────────────────────────────────────────────
# Step Execution
# ──────────────────────────────────────────────


def execute_step(step: dict) -> dict:
    """
    Execute a single pipeline step as a subprocess.
    Returns a result dict with status, duration, and error details.
    """
    name = step["name"]
    logger.info("─" * 50)
    logger.info("STEP: %s", name)
    logger.info("  Description: %s", step["description"])
    logger.info("  Command: %s", " ".join(step["command"]))
    logger.info("─" * 50)

    start_time = time.time()
    result = {
        "name": name,
        "status": "unknown",
        "duration_seconds": 0.0,
        "error": None,
    }

    try:
        process = subprocess.run(
            step["command"],
            capture_output=False,  # Stream output directly
            text=True,
            timeout=1800,  # 30 minute timeout per step
        )

        result["duration_seconds"] = round(time.time() - start_time, 2)

        if process.returncode == 0:
            result["status"] = "SUCCESS"
            logger.info("✓ Step '%s' completed successfully (%.1fs)", name, result["duration_seconds"])
        else:
            result["status"] = "FAILED"
            result["error"] = f"Exit code: {process.returncode}"
            logger.error("✗ Step '%s' failed with exit code %d", name, process.returncode)

    except subprocess.TimeoutExpired:
        result["status"] = "TIMEOUT"
        result["duration_seconds"] = round(time.time() - start_time, 2)
        result["error"] = "Step timed out after 1800 seconds"
        logger.error("✗ Step '%s' timed out!", name)

    except FileNotFoundError as exc:
        result["status"] = "NOT_FOUND"
        result["duration_seconds"] = round(time.time() - start_time, 2)
        result["error"] = str(exc)
        logger.error("✗ Step '%s' command not found: %s", name, exc)

    except Exception as exc:
        result["status"] = "ERROR"
        result["duration_seconds"] = round(time.time() - start_time, 2)
        result["error"] = str(exc)
        logger.exception("✗ Step '%s' encountered an unexpected error: %s", name, exc)

    return result


# ──────────────────────────────────────────────
# Pipeline Execution
# ──────────────────────────────────────────────


def run_pipeline() -> list[dict]:
    """Execute all pipeline steps in sequence."""
    logger.info("=" * 60)
    logger.info("E-Commerce Data Lakehouse — Full Pipeline Execution")
    logger.info("  Started at: %s", datetime.utcnow().isoformat())
    logger.info("  Steps: %d", len(PIPELINE_STEPS))
    logger.info("=" * 60)

    pipeline_start = time.time()
    results = []

    for i, step in enumerate(PIPELINE_STEPS, 1):
        logger.info("\n[%d/%d] Starting: %s", i, len(PIPELINE_STEPS), step["name"])

        result = execute_step(step)
        results.append(result)

        # Stop pipeline if a required step fails
        if result["status"] != "SUCCESS" and step.get("required", True):
            logger.error(
                "Required step '%s' failed. Halting pipeline.", step["name"]
            )
            break

        if result["status"] != "SUCCESS" and not step.get("required", True):
            logger.warning(
                "Optional step '%s' failed. Continuing pipeline.", step["name"]
            )

    # ─── Summary ─────────────────────────────
    total_duration = round(time.time() - pipeline_start, 2)

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)

    all_success = True
    for r in results:
        status_icon = "✓" if r["status"] == "SUCCESS" else "✗"
        logger.info(
            "  %s %-30s  %s  (%.1fs)",
            status_icon, r["name"], r["status"], r["duration_seconds"],
        )
        if r["error"]:
            logger.info("    Error: %s", r["error"])
        if r["status"] != "SUCCESS":
            all_success = False

    logger.info("─" * 60)
    logger.info("  Total Duration: %.1f seconds", total_duration)
    logger.info("  Overall Status: %s", "SUCCESS" if all_success else "FAILED")
    logger.info("=" * 60)

    return results


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────


def main() -> None:
    """Run the full data lakehouse pipeline."""
    results = run_pipeline()

    # Exit with appropriate code
    failed = [r for r in results if r["status"] != "SUCCESS"]
    required_failed = any(
        r["status"] != "SUCCESS"
        for r, step in zip(results, PIPELINE_STEPS)
        if step.get("required", True)
    )

    if required_failed:
        logger.error("Pipeline failed — required steps did not complete.")
        sys.exit(1)
    elif failed:
        logger.warning("Pipeline completed with warnings — optional steps failed.")
        sys.exit(0)
    else:
        logger.info("Pipeline completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
