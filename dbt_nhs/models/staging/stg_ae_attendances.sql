{{ config(materialized='view') }}

/*
  stg_ae_attendances
  ------------------
  Staging view over raw.ae_attendances.
  Renames columns to business-friendly names, adds a 95% compliance flag,
  and categorises performance into traffic-light bands.
*/

with source as (
    select * from {{ source('raw', 'ae_attendances') }}
),

staged as (
    select
        id                                              as ae_id,
        period_date                                     as reporting_month,
        trust_code,
        trust_name,
        ae_type,

        total_attendances,
        seen_within_4hrs,
        emergency_admissions,

        compliance_rate_pct,
        below_95_target,
        admission_rate_pct,

        -- Traffic-light performance band
        case
            when compliance_rate_pct >= 95 then 'Green'
            when compliance_rate_pct >= 85 then 'Amber'
            else 'Red'
        end                                             as performance_band,

        loaded_at

    from source
    where total_attendances > 0
)

select * from staged
