"""
ingest.py
---------
Downloads NHS England open data CSVs to data/raw/.
Falls back to generating realistic synthetic data if URLs are unavailable.

Usage:
    python ingest.py
"""
import os
import sys
import logging
import requests
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

NHS_AE_URL = (
    "https://files.digital.nhs.uk/assets/ods/current/ae_attendances.csv"
)

TRUSTS = [
    ("R0A", "Manchester University NHS Foundation Trust"),
    ("RRK", "University Hospitals Birmingham NHS Foundation Trust"),
    ("RJ1", "Guy's and St Thomas' NHS Foundation Trust"),
    ("RQM", "Chelsea and Westminster Hospital NHS Foundation Trust"),
    ("RP6", "Croydon Health Services NHS Trust"),
    ("RTG", "Derby Teaching Hospitals NHS Foundation Trust"),
    ("RWE", "University Hospitals of Leicester NHS Trust"),
    ("RXN", "Lancashire Teaching Hospitals NHS Foundation Trust"),
]

os.makedirs("../data/raw", exist_ok=True)


def download_csv(url: str, dest: str) -> bool:
    """Attempt to download a CSV. Returns True on success."""
    log.info(f"Attempting download: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(dest, "wb") as f:
            f.write(response.content)
        log.info(f"Downloaded successfully → {dest}")
        return True
    except Exception as exc:
        log.warning(f"Download failed ({exc}). Will use synthetic data instead.")
        return False


def generate_ae_data(dest: str) -> None:
    """
    Generate synthetic but realistic A&E attendance data.
    Mimics NHS England monthly A&E statistics format.
    """
    rng = np.random.default_rng(seed=42)
    periods = pd.period_range("2022-01", periods=28, freq="M")
    rows = []

    for period in periods:
        for code, name in TRUSTS:
            # Seasonal variation: higher winter attendance
            month = period.month
            seasonal_factor = 1.0 + 0.12 * np.cos((month - 1) * np.pi / 6)
            base = int(rng.normal(9500, 1400) * seasonal_factor)
            total = max(base, 2000)

            # Compliance degrades slightly in winter
            compliance_base = rng.uniform(0.74, 0.97)
            winter_penalty = 0.06 if month in (12, 1, 2) else 0.0
            compliance = max(compliance_base - winter_penalty, 0.55)

            seen_4h = int(total * compliance)
            admissions = int(total * rng.uniform(0.18, 0.32))

            rows.append({
                "period":               str(period),
                "trust_code":           code,
                "trust_name":           name,
                "ae_type":              "Type 1",
                "total_attendances":    total,
                "seen_within_4hrs":     seen_4h,
                "emergency_admissions": admissions,
            })

    df = pd.DataFrame(rows)
    df.to_csv(dest, index=False)
    log.info(f"Synthetic A&E data created: {len(df):,} rows → {dest}")


def generate_bed_data(dest: str) -> None:
    """
    Generate synthetic NHS bed availability and occupancy data.
    Mimics NHS England quarterly bed statistics format.
    """
    rng = np.random.default_rng(seed=99)
    quarters = pd.period_range("2022Q1", periods=7, freq="Q")
    rows = []

    for quarter in quarters:
        for code, name in TRUSTS:
            available = int(rng.normal(650, 90))
            available = max(available, 150)
            occupancy = rng.uniform(0.79, 0.97)
            # Winter quarters push occupancy higher
            if quarter.month in (10, 1):
                occupancy = min(occupancy + 0.04, 0.99)
            occupied = int(available * occupancy)

            rows.append({
                "quarter":         str(quarter),
                "trust_code":      code,
                "trust_name":      name,
                "beds_available":  available,
                "beds_occupied":   occupied,
            })

    df = pd.DataFrame(rows)
    df.to_csv(dest, index=False)
    log.info(f"Synthetic bed data created: {len(df):,} rows → {dest}")


def main() -> None:
    ae_dest   = "../data/raw/ae_attendances.csv"
    beds_dest = "../data/raw/bed_occupancy.csv"

    log.info("=== NHS Pipeline — Ingestion ===")

    if not download_csv(NHS_AE_URL, ae_dest):
        generate_ae_data(ae_dest)

    generate_bed_data(beds_dest)

    log.info("=== Ingestion complete ===")


if __name__ == "__main__":
    main()
