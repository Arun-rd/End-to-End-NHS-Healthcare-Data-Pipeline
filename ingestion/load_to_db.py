"""
load_to_db.py
-------------
Loads cleaned Parquet files into PostgreSQL raw schema using SQLAlchemy.
Designed to be idempotent: truncates existing data before loading (full refresh).

Usage:
    python load_to_db.py
"""
import logging
import sys
import pandas as pd
from sqlalchemy import text
from config import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def truncate_and_load(parquet_path: str, schema: str, table: str, engine) -> int:
    """
    Truncate table and load fresh data from parquet_path.
    Returns number of rows loaded.
    """
    log.info(f"Loading {parquet_path} → {schema}.{table}")
    df = pd.read_parquet(parquet_path)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY"))

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    log.info(f"  Loaded {len(df):,} rows into {schema}.{table}")
    return len(df)


def verify_load(schema: str, table: str, engine) -> int:
    """Return row count from the database table for verification."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
        count = result.scalar()
    log.info(f"  Verified: {count:,} rows in {schema}.{table}")
    return count


def main() -> None:
    log.info("=== NHS Pipeline — Load to PostgreSQL ===")
    engine = get_engine()

    try:
        ae_rows   = truncate_and_load("../data/clean/ae_attendances.parquet",  "raw", "ae_attendances", engine)
        beds_rows = truncate_and_load("../data/clean/bed_occupancy.parquet",   "raw", "bed_occupancy",  engine)

        verify_load("raw", "ae_attendances", engine)
        verify_load("raw", "bed_occupancy",  engine)

        log.info(f"=== Load complete: {ae_rows + beds_rows:,} total rows loaded ===")

    except Exception as exc:
        log.error(f"Load failed: {exc}")
        log.error("Check your .env credentials and that PostgreSQL is running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
