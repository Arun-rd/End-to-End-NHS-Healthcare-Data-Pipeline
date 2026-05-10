"""
run_pipeline.py
---------------
Orchestrates the full ingestion pipeline in one command:
    ingest → clean → load

Usage:
    python run_pipeline.py
"""
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    log.info("========================================")
    log.info("  NHS Healthcare Data Pipeline — START  ")
    log.info("========================================")

    # Step 1: Ingest
    log.info("--- Step 1/3: Ingestion ---")
    import ingest
    ingest.main()

    # Step 2: Clean
    log.info("--- Step 2/3: Cleaning ---")
    import clean
    clean.main()

    # Step 3: Load
    log.info("--- Step 3/3: Load to PostgreSQL ---")
    import load_to_db
    load_to_db.main()

    log.info("========================================")
    log.info("  Pipeline complete. Run dbt next:      ")
    log.info("  cd ../dbt_nhs && dbt run && dbt test  ")
    log.info("========================================")


if __name__ == "__main__":
    main()
