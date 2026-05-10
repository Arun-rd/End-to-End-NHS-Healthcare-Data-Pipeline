# Power BI Setup Guide

## Connect to PostgreSQL

1. Open **Power BI Desktop**
2. **Get Data** → **PostgreSQL database**
3. Enter:
   - Server: `localhost`
   - Database: `nhs_pipeline`
4. Select **DirectQuery** or **Import** (Import recommended for portfolio)
5. Navigate to **marts** schema → select `mart_nhs_kpis` → **Load**

---

## Recommended Visuals

### Page 1 — A&E Performance Overview

| Visual | Config |
|--------|--------|
| Line chart | X-axis: `reporting_month`, Y-axis: `compliance_rate_pct`, Legend: `trust_name` |
| Card | Value: `Avg Compliance %` (DAX measure) |
| Card | Value: `Trusts Below 95% Target` (DAX measure) |
| Slicer | Field: `trust_name` |

### Page 2 — Bed Occupancy

| Visual | Config |
|--------|--------|
| Bar chart | X-axis: `trust_name`, Y-axis: `occupancy_rate_pct` |
| Conditional formatting | `occupancy_risk_band` → Green / Amber / Red |
| Card | Value: `Avg Occupancy %` (DAX measure) |
| Card | Value: `High Occupancy Trusts` (DAX measure) |

### Page 3 — Trust Drilldown

| Visual | Config |
|--------|--------|
| Table | All columns, filtered by slicer |
| Scatter plot | X: `compliance_rate_pct`, Y: `occupancy_rate_pct`, Size: `total_attendances` |

---

## DAX Measures

```dax
Avg Compliance % =
    AVERAGE(mart_nhs_kpis[compliance_rate_pct])

Trusts Below Target =
    COUNTROWS(
        FILTER(mart_nhs_kpis, mart_nhs_kpis[below_95_target] = TRUE())
    )

Avg Occupancy % =
    AVERAGE(mart_nhs_kpis[occupancy_rate_pct])

High Occupancy Trusts =
    COUNTROWS(
        FILTER(mart_nhs_kpis, mart_nhs_kpis[high_occupancy] = TRUE())
    )

Total Attendances (Selected) =
    SUM(mart_nhs_kpis[total_attendances])

MoM Compliance Change % =
VAR CurrentMonth = MAX(mart_nhs_kpis[reporting_month])
VAR PrevMonth = EDATE(CurrentMonth, -1)
VAR CurrentVal = CALCULATE(
    AVERAGE(mart_nhs_kpis[compliance_rate_pct]),
    mart_nhs_kpis[reporting_month] = CurrentMonth
)
VAR PrevVal = CALCULATE(
    AVERAGE(mart_nhs_kpis[compliance_rate_pct]),
    mart_nhs_kpis[reporting_month] = PrevMonth
)
RETURN
    DIVIDE(CurrentVal - PrevVal, PrevVal) * 100
```
