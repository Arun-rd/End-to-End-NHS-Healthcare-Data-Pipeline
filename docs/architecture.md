# Pipeline Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCES                        │
│  NHS England Open Data (digital.nhs.uk)                 │
│  • A&E Attendances (monthly CSV)                        │
│  • Bed Availability & Occupancy (quarterly CSV)         │
└───────────────────────┬─────────────────────────────────┘
                        │ requests / HTTP
┌───────────────────────▼─────────────────────────────────┐
│                   INGESTION LAYER                       │
│  ingestion/ingest.py                                    │
│  • Downloads CSVs from NHS England                      │
│  • Falls back to synthetic data if URL unavailable      │
│  • Output: data/raw/*.csv                               │
└───────────────────────┬─────────────────────────────────┘
                        │ pandas
┌───────────────────────▼─────────────────────────────────┐
│                   CLEANING LAYER                        │
│  ingestion/clean.py                                     │
│  • Standardises column names                            │
│  • Removes duplicates and nulls                         │
│  • Coerces and validates numeric types                  │
│  • Derives KPI columns (compliance %, occupancy %)      │
│  • Output: data/clean/*.parquet                         │
└───────────────────────┬─────────────────────────────────┘
                        │ sqlalchemy / psycopg2
┌───────────────────────▼─────────────────────────────────┐
│                   WAREHOUSE LAYER                       │
│  PostgreSQL — nhs_pipeline database                     │
│  • raw.ae_attendances                                   │
│  • raw.bed_occupancy                                    │
└───────────────────────┬─────────────────────────────────┘
                        │ dbt
┌───────────────────────▼─────────────────────────────────┐
│                TRANSFORMATION LAYER                     │
│  dbt_nhs project                                        │
│  staging/ (views)                                       │
│  • stg_ae_attendances — rename, band, flag              │
│  • stg_bed_occupancy  — rename, risk band               │
│  marts/ (tables)                                        │
│  • mart_nhs_kpis — joined, final, Power BI ready        │
└───────────────────────┬─────────────────────────────────┘
                        │ PostgreSQL connector
┌───────────────────────▼─────────────────────────────────┐
│                PRESENTATION LAYER                       │
│  Power BI Desktop                                       │
│  • Page 1: A&E Performance (compliance trend)           │
│  • Page 2: Bed Occupancy (risk by trust)                │
│  • Page 3: Trust Drilldown (scatter + table)            │
└─────────────────────────────────────────────────────────┘
```

## Design Decisions

**Why dbt?**  
dbt brings software engineering practices (version control, testing, documentation) to SQL transformations. Every model is tested and documented, and `dbt docs serve` generates an interactive lineage graph — exactly what data engineering teams use in production.

**Why PostgreSQL over a cloud warehouse?**  
Running locally removes cloud cost for a portfolio project while demonstrating the same skills. The dbt models are database-agnostic and would work unchanged on BigQuery, Snowflake, or Redshift with a profile change.

**Why Parquet as intermediate format?**  
Parquet is columnar, compressed, and the standard format for data lakes. Storing cleaned data as Parquet before loading to Postgres demonstrates awareness of modern data engineering patterns.
