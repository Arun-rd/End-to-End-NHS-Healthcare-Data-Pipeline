-- Custom dbt test: compliance_rate_pct must always be between 0 and 100.
-- Returns rows that violate this — test passes when 0 rows returned.

select *
from {{ ref('mart_nhs_kpis') }}
where compliance_rate_pct < 0
   or compliance_rate_pct > 100
