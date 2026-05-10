-- ============================================================
-- 02_create_schemas.sql
-- Run connected to nhs_pipeline:
--   psql -U postgres -d nhs_pipeline -f sql/02_create_schemas.sql
-- ============================================================

\c nhs_pipeline;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================
-- RAW TABLES
-- Exact mirror of cleaned CSVs — no business logic here
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.ae_attendances (
    id                   SERIAL PRIMARY KEY,
    period               VARCHAR(10)    NOT NULL,
    period_date          DATE           NOT NULL,
    trust_code           VARCHAR(10)    NOT NULL,
    trust_name           VARCHAR(250),
    ae_type              VARCHAR(50),
    total_attendances    INTEGER        CHECK (total_attendances >= 0),
    seen_within_4hrs     INTEGER        CHECK (seen_within_4hrs >= 0),
    emergency_admissions INTEGER        CHECK (emergency_admissions >= 0),
    compliance_rate_pct  NUMERIC(6,2),
    below_95_target      BOOLEAN,
    admission_rate_pct   NUMERIC(6,2),
    loaded_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.bed_occupancy (
    id                  SERIAL PRIMARY KEY,
    quarter             VARCHAR(10)    NOT NULL,
    quarter_date        DATE           NOT NULL,
    trust_code          VARCHAR(10)    NOT NULL,
    trust_name          VARCHAR(250),
    beds_available      INTEGER        CHECK (beds_available >= 0),
    beds_occupied       INTEGER        CHECK (beds_occupied >= 0),
    occupancy_rate_pct  NUMERIC(6,2),
    high_occupancy      BOOLEAN,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Useful indexes for dbt join performance
CREATE INDEX IF NOT EXISTS idx_ae_trust_period   ON raw.ae_attendances (trust_code, period_date);
CREATE INDEX IF NOT EXISTS idx_beds_trust_quarter ON raw.bed_occupancy  (trust_code, quarter_date);

COMMENT ON TABLE raw.ae_attendances IS 'NHS England A&E Attendances — monthly, by trust';
COMMENT ON TABLE raw.bed_occupancy  IS 'NHS England Bed Availability & Occupancy — quarterly, by trust';
