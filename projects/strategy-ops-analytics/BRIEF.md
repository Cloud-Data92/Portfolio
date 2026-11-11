# Leadership readout — headcount and Q4 objectives

**As of October 31, 2026 · synthetic data · every figure traces to a file in [`outputs/`](outputs/)**

## The short version

We are 40 people under plan and the gap is growing. More than half of it is one organization, Site Operations West, where attrition has risen by half since spring. At the current pace we finish the year 45 under plan. Only 2 of 16 Q4 objectives are on track a third of the way through the quarter.

## What needs a decision

**1. Site Operations West needs a retention response, not only more hiring.**
It is 23 under plan (141 vs. 164) and annualized attrition has gone from 29% to 44% in six months. It also produced two of the five KPI alerts this quarter: an on-time delivery drop on October 6 and a cycle-time spike on October 27. Hiring 25% faster and cutting attrition by a quarter each close about the same amount of its gap by March (29 under plan either way, against 35 on the current trend). Neither closes it alone.

**2. A hiring freeze is the expensive option.**
Organization-wide, a freeze takes the year-end gap from 45 to 92, and to 173 by the end of March. If a freeze is on the table, it should exempt Site Operations West, where a freeze alone widens the March gap from 35 to 57.

**3. Seven objectives are off track and should be re-scoped or re-resourced now.**
There are nine weeks left in the quarter. One further objective (Demand Forecasting, OBJ-02-1) cannot be assessed: two of its three key results have had no update in over two weeks.

## What is going well

- Reliability Engineering and Demand Forecasting are above plan and stable.
- Site Operations East has cut attrition from 26% to 15% and both of its objectives are on track.
- All five KPI anomalies this quarter were single-day events; none repeated the following day.

## How much to trust these numbers

- Headcount excludes 3 people whose records point at an org that does not exist. Small, but it should be fixed at the source.
- 94 workforce events were entered more than ten days late, so recent month-end figures will be restated slightly upward or downward on the next run.
- One objective's key-result weights sum to 1.2. The roll-up is normalized, so the percentage is right, but the owner should confirm the intended weighting.
- The scenario projections assume constant monthly rates taken from the last six months. They are for comparing options, not for setting targets.

## Method, in one line each

Headcount is derived from the hire/transfer/termination log as of each month end ([01](sql/01_headcount_plan_vs_actual.sql)). Objective status compares progress with the share of the quarter elapsed at the last check-in ([02](sql/02_okr_progress_and_pacing.sql)). Alerts compare each day with the same weekday over the prior eight weeks ([03](sql/03_kpi_anomaly_alerts.sql)). Scenarios scale each org's own recent hiring and attrition ([04](sql/04_headcount_what_if_scenarios.sql)). Data-quality counts come from the scorecard ([05](sql/05_data_quality_checks.sql)).
