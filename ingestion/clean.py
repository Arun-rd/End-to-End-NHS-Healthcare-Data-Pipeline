"""
clean.py
--------
Reads raw NHS CSVs, applies Pandas cleaning and validation,
derives KPI columns, and outputs Parquet files to data/clean/.

Usage:
    python clean.py
"""
import logging
import os
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

os.makedirs("../data/clean", exist_ok=True)


def _standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and underscore column names."""
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def _log_quality_report(df: pd.DataFrame, label: str) -> None:
    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    if null_cols.empty:
        log.info(f"{label}: no nulls remaining")
    else:
        log.warning(f"{label}: nulls after cleaning:\n{null_cols}")

    dupes = df.duplicated().sum()
    if dupes:
        log.warning(f"{label}: {dupes} duplicate rows remaining")


def clean_ae(src: str, dest: str) -> pd.DataFrame:
    """
    Clean A&E attendances dataset.

    Transformations:
    - Standardise column names
    - Drop full duplicates
    - Drop rows missing period or trust_code
    - Parse period string to date
    - Coerce numeric columns, clip negatives to 0
    - Derive compliance_rate_pct and below_95_target flag
    - Trim string whitespace
    """
    log.info(f"Cleaning A&E data: {src}")
    df = pd.read_csv(src)
    initial = len(df)

    df = _standardise_columns(df)
    df = df.drop_duplicates()
    df = df.dropna(subset=["period", "trust_code"])

    df["period_date"] = pd.PeriodIndex(df["period"], freq="M").to_timestamp()

    numeric_cols = ["total_attendances", "seen_within_4hrs", "emergency_admissions"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").clip(lower=0)

    # Remove rows where total_attendances is null or zero (no activity)
    df = df[df["total_attendances"].gt(0)]

    # Derived KPI: 4-hour compliance rate
    df["compliance_rate_pct"] = (
        df["seen_within_4hrs"] / df["total_attendances"] * 100
    ).round(2)

    # Flag: NHS 95% standard
    df["below_95_target"] = df["compliance_rate_pct"] < 95.0

    # Derived KPI: emergency admission rate
    df["admission_rate_pct"] = (
        df["emergency_admissions"] / df["total_attendances"] * 100
    ).round(2)

    df["trust_code"] = df["trust_code"].str.strip().str.upper()
    df["trust_name"] = df["trust_name"].str.strip()

    _log_quality_report(df, "A&E")
    log.info(f"A&E cleaned: {initial:,} → {len(df):,} rows")

    df.to_parquet(dest, index=False)
    log.info(f"Saved → {dest}")
    return df


def clean_beds(src: str, dest: str) -> pd.DataFrame:
    """
    Clean NHS bed availability and occupancy dataset.

    Transformations:
    - Standardise column names
    - Drop duplicates and rows missing key identifiers
    - Parse quarter to timestamp
    - Coerce and clip numeric columns
    - Derive occupancy_rate_pct and high_occupancy flag (>92%)
    """
    log.info(f"Cleaning bed data: {src}")
    df = pd.read_csv(src)
    initial = len(df)

    df = _standardise_columns(df)
    df = df.drop_duplicates()
    df = df.dropna(subset=["quarter", "trust_code"])

    df["quarter_date"] = pd.PeriodIndex(df["quarter"], freq="Q").to_timestamp()

    for col in ["beds_available", "beds_occupied"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").clip(lower=0)

    df = df[df["beds_available"].gt(0)]

    df["occupancy_rate_pct"] = (
        df["beds_occupied"] / df["beds_available"] * 100
    ).round(2)

    # NHS guidance: >92% occupancy increases infection risk
    df["high_occupancy"] = df["occupancy_rate_pct"] > 92.0

    df["trust_code"] = df["trust_code"].str.strip().str.upper()
    df["trust_name"] = df["trust_name"].str.strip()

    _log_quality_report(df, "Beds")
    log.info(f"Beds cleaned: {initial:,} → {len(df):,} rows")

    df.to_parquet(dest, index=False)
    log.info(f"Saved → {dest}")
    return df


def main() -> None:
    log.info("=== NHS Pipeline — Cleaning ===")
    clean_ae("../data/raw/ae_attendances.csv",  "../data/clean/ae_attendances.parquet")
    clean_beds("../data/raw/bed_occupancy.csv", "../data/clean/bed_occupancy.parquet")
    log.info("=== Cleaning complete ===")


if __name__ == "__main__":
    main()
