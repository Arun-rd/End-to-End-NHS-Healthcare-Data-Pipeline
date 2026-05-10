{{ config(materialized='view') }}

/*
  stg_bed_occupancy
  -----------------
  Staging view over raw.bed_occupancy.
  Cleans column names and adds an occupancy risk band
  based on NHS England's published guidance thresholds.
*/

with source as (
    select * from {{ source('raw', 'bed_occupancy') }}
),

staged as (
    select
        quarter_date,
        trust_code,
        trust_name,

        beds_available,
        beds_occupied,
        occupancy_rate_pct,
        high_occupancy,

        -- NHS guidance occupancy risk bands
        case
            when occupancy_rate_pct < 85  then 'Low risk'
            when occupancy_rate_pct < 92  then 'Moderate risk'
            else 'High risk'
        end                             as occupancy_risk_band,

        loaded_at

    from source
    where beds_available > 0
)

select * from staged
