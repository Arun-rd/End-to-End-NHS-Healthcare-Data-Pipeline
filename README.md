# NHS Healthcare Data Pipeline

An end-to-end data engineering portfolio project demonstrating a production-grade pipeline built on public NHS England open data.

**Tech stack:** Python · Pandas · PostgreSQL · dbt · Power BI

---

## What this project does

Ingests two NHS England open datasets (A&E waiting times and bed occupancy), cleans and validates them with Python, loads them into a PostgreSQL data warehouse, applies dbt transformations with automated data quality tests, and surfaces KPI dashboards in Power BI.

The datasets and metrics mirror the kind of clinical performance reporting I built at Modality LLP (NHS Community Services) — now demonstrated end-to-end on public data.

---

## Architecture

```
NHS England Open Data (CSV)
         │
         ▼
 Python ingestion + Pandas cleaning
         │
         ▼
 PostgreSQL (raw schema)
         │
         ▼
 dbt transformations + quality tests
  └── staging/  (views: rename, validate, band)
  └── marts/    (tables: joined KPI mart)
         │
         ▼
 Power BI Dashboard
  └── A&E compliance trend
  └── Bed occupancy risk by trust
  └── Trust drilldown scatter
```

See [`docs/architecture.md`](docs/architecture.md) for full detail and design decisions.

---

## Datasets

| Dataset | Publisher | Frequency | Rows |
|---|---|---|---|
| [A&E Attendances & Emergency Admissions](https://digital.nhs.uk/data-and-information/publications/statistical/ae-waiting-times-and-activity) | NHS England | Monthly | ~2,800 |
| [NHS Bed Availability & Occupancy](https://digital.nhs.uk/data-and-information/publications/statistical/nhs-beds-timeseries) | NHS England | Quarterly | ~400 |

Both published under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence).

---

## Project structure

```
nhs-pipeline/
├── ingestion/
│   ├── config.py           # DB connection (reads from .env)
│   ├── ingest.py           # Download NHS CSVs with synthetic fallback
│   ├── clean.py            # Pandas cleaning, validation, KPI derivation
│   ├── load_to_db.py       # Load Parquet → PostgreSQL
│   └── run_pipeline.py     # One-command orchestrator
├── sql/
│   ├── 01_create_database.sql
│   └── 02_create_schemas.sql
├── dbt_nhs/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_ae_attendances.sql
│   │   │   └── stg_bed_occupancy.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       └── mart_nhs_kpis.sql
│   ├── tests/
│   │   ├── assert_compliance_between_0_and_100.sql
│   │   └── assert_no_future_dates.sql
│   ├── dbt_project.yml
│   └── profiles.yml.example
├── docs/
│   ├── architecture.md
│   └── powerbi_setup.md
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Quickstart

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ running locally
- Power BI Desktop (free, Windows)

### 1 — Clone and install

```bash
git clone https://github.com/Arun-rd/nhs-pipeline.git
cd nhs-pipeline
pip install -r requirements.txt
```

### 2 — Configure credentials

```bash
cp .env.example .env
# Edit .env — add your PostgreSQL password
```

### 3 — Create the database schemas

```bash
psql -U postgres -f sql/01_create_database.sql
psql -U postgres -d nhs_pipeline -f sql/02_create_schemas.sql
```

### 4 — Run the full ingestion pipeline

```bash
cd ingestion
python run_pipeline.py
```

This runs ingest → clean → load in one step. Expected output:

```
2026-05-09 | INFO     | === NHS Pipeline — START ===
2026-05-09 | INFO     | Synthetic A&E data created: 224 rows → data/raw/ae_attendances.csv
2026-05-09 | INFO     | A&E cleaned: 224 → 224 rows
2026-05-09 | INFO     | Loaded 224 rows into raw.ae_attendances
2026-05-09 | INFO     | Loaded 56 rows into raw.bed_occupancy
2026-05-09 | INFO     | === Pipeline complete ===
```

### 5 — Run dbt transformations

```bash
# Copy profiles.yml.example to ~/.dbt/profiles.yml and fill in credentials
cd ../dbt_nhs
dbt debug          # verify connection
dbt run            # build all models
dbt test           # run data quality tests
dbt docs generate && dbt docs serve   # view lineage graph at localhost:8080
```

### 6 — Connect Power BI

See [`docs/powerbi_setup.md`](docs/powerbi_setup.md) for step-by-step instructions and DAX measures.

---

## KPIs tracked

| Metric | Definition | NHS Standard |
|---|---|---|
| A&E compliance rate | % patients seen within 4 hours | ≥ 95% |
| Performance band | Green ≥95% / Amber ≥85% / Red <85% | — |
| Emergency admission rate | Emergency admissions / total attendances | — |
| Bed occupancy rate | Beds occupied / beds available | < 92% |
| Occupancy risk band | Low <85% / Moderate <92% / High ≥92% | — |

---

## Data quality tests (dbt)

| Test | Model | Type |
|---|---|---|
| `ae_id` is unique and not null | `stg_ae_attendances` | Generic |
| `compliance_rate_pct` between 0–100 | `mart_nhs_kpis` | Custom SQL |
| `performance_band` in accepted values | `stg_ae_attendances` | Generic |
| No future reporting dates | `mart_nhs_kpis` | Custom SQL |
| `occupancy_risk_band` in accepted values | `stg_bed_occupancy` | Generic |

---

## Skills demonstrated

`Python` `Pandas` `PostgreSQL` `SQLAlchemy` `dbt` `Data modelling` `ETL pipeline design` `Data quality testing` `Parquet` `SQL` `Power BI` `DAX` `NHS domain knowledge`

---

## Author

**Arun Kumar Ravi** — BI Developer → Data Engineer  
[LinkedIn](https://www.linkedin.com/in/arun-ravi-07/) · [GitHub](https://github.com/Arun-rd)
