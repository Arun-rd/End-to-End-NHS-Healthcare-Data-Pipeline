-- Custom dbt test: no reporting months in the future.
-- Returns rows that violate this — test passes when 0 rows returned.

select *
from {{ ref('mart_nhs_kpis') }}
where reporting_month > current_date
