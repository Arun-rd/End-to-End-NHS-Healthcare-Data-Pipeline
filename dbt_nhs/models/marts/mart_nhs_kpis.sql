{{ config(materialized='table') }}

/*
  mart_nhs_kpis
  -------------
  Final KPI mart consumed by Power BI.
  One row per trust per month, joining A&E metrics with the
  nearest available bed occupancy quarter.

  Key metrics exposed:
    - A&E compliance rate (4-hour standard)
    - A&E performance band (Green / Amber / Red)
    - Emergency admission rate
    - Bed occupancy rate and risk band
*/

with ae as (
    select * from {{ ref('stg_ae_attendances') }}
),

beds as (
    select * from {{ ref('stg_bed_occupancy') }}
),

-- Get the most recent beds snapshot for each trust (to handle missing quarters)
latest_beds as (
    select distinct on (trust_code)
        trust_code,
        quarter_date,
        beds_available,
        beds_occupied,
        occupancy_rate_pct,
        high_occupancy,
        occupancy_risk_band
    from beds
    order by trust_code, quarter_date desc
),

joined as (
    select
        -- Dimensions
        ae.reporting_month,
        ae.trust_code,
        ae.trust_name,
        ae.ae_type,

        -- A&E metrics
        ae.total_attendances,
        ae.seen_within_4hrs,
        ae.emergency_admissions,
        ae.compliance_rate_pct,
        ae.below_95_target,
        ae.admission_rate_pct,
        ae.performance_band,

        -- Bed metrics (quarter join: match month to its quarter)
        coalesce(
            b_exact.beds_available,
            lb.beds_available
        )                                               as beds_available,

        coalesce(
            b_exact.beds_occupied,
            lb.beds_occupied
        )                                               as beds_occupied,

        coalesce(
            b_exact.occupancy_rate_pct,
            lb.occupancy_rate_pct
        )                                               as occupancy_rate_pct,

        coalesce(
            b_exact.high_occupancy,
            lb.high_occupancy
        )                                               as high_occupancy,

        coalesce(
            b_exact.occupancy_risk_band,
            lb.occupancy_risk_band
        )                                               as occupancy_risk_band,

        -- Helper flag: both datasets present for this trust+period
        (b_exact.trust_code is not null)                as beds_data_available

    from ae

    -- Exact quarter match
    left join beds b_exact
        on  ae.trust_code = b_exact.trust_code
        and date_trunc('quarter', ae.reporting_month) = b_exact.quarter_date

    -- Fallback: latest available beds snapshot
    left join latest_beds lb
        on  ae.trust_code = lb.trust_code
        and b_exact.trust_code is null
)

select * from joined
order by reporting_month desc, trust_name
