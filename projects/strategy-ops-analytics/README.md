# 🧭 Strategy & Operations Analytics — Headcount, OKRs, KPI Alerts, What-If

**The reporting a Chief-of-Staff or StratOps team runs on: are we staffed to plan, are the quarter's objectives on pace, did any metric move more than it should, and where do we land under different assumptions.** Five BigQuery analyses over one synthetic organization, executed locally with committed results, ending in a [one-page leadership readout](BRIEF.md).

My professional work has been operational analytics for a delivery network; this project applies the same habits (define the grain, derive rather than hand-maintain, validate before publishing) to the planning-and-performance side of a business.

> Original SQL against an invented schema and seeded synthetic data. No employer code, data, org structure or targets. See [NOTICE.md](../../NOTICE.md).

```bash
python3 generate_data.py                                              # seeded, standard library only
uv run --with duckdb --with sqlglot --with pytz python run.py         # or pip install the three
```

The `.sql` files are BigQuery Standard SQL and are the source of truth. `run.py` translates each statement to DuckDB with `sqlglot` and writes [`outputs/`](outputs/); nothing is hand-edited for the local run.

---

## The analyses

| # | File | Leadership question | SQL techniques |
|---|---|---|---|
| 01 | [Headcount, plan vs. actual, attrition](sql/01_headcount_plan_vs_actual.sql) | How many people do we have against plan, and where are we losing them? | Point-in-time state from an event log, `GENERATE_DATE_ARRAY` + `UNNEST`, `QUALIFY ROW_NUMBER`, curated tables, annualized rates |
| 02 | [OKR progress and pacing](sql/02_okr_progress_and_pacing.sql) | Which objectives are behind for this point in the quarter, and which have gone quiet? | Latest-record selection, direction-agnostic progress, weighted roll-up, staleness rules |
| 03 | [Automated KPI alerts](sql/03_kpi_anomaly_alerts.sql) | Which metrics moved more than normal variation explains? Is any data missing? | Named windows, same-weekday baselines, residual z-scores, `LAG` gap detection |
| 04 | [Headcount what-if scenarios](sql/04_headcount_what_if_scenarios.sql) | Where do we land against plan if nothing changes, if we freeze hiring, if we fix retention? | Scenario table × closed-form projection, `GENERATE_ARRAY`, `POW`, pivot by conditional aggregation |
| 05 | [Data-quality scorecard](sql/05_data_quality_checks.sql) | Can today's numbers be trusted? | Nine counted checks with severity and stated downstream impact |

Each file opens with the business problem and closes with its design decisions.

---

## What the results say

All figures are from the committed outputs, as of 2026-10-31 in the synthetic data.

**One org explains most of the headcount gap.** The organization is 40 under plan at Q3 (951 actual vs. 991 planned). Site Operations West is 23 of those 40: 14% under its plan, with annualized attrition up from 29% to 44% over the last six months ([01, queries 1 and 2](outputs/)).

| Org | Q3 plan | Q3 actual | Variance | Attrition, prior 6 mo | Attrition, last 6 mo |
|---|---|---|---|---|---|
| Site Operations West | 164 | 141 | −23 (−14.0%) | 29.3% | 43.5% |
| Program Management | 62 | 57 | −5 (−8.1%) | 14.2% | 21.1% |
| Network Engineering | 165 | 154 | −11 (−6.7%) | 11.2% | 11.8% |
| Site Operations East | 200 | 188 | −12 (−6.0%) | 26.4% | 15.0% |

**The gap widens under every scenario, and freezing hiring doubles it.** Projected organization-wide gap to plan at year end ([04](outputs/04_headcount_what_if_scenarios__q1.csv)):

| Scenario | Projected Dec 31 | Plan | Gap Dec 31 | Gap Mar 31 |
|---|---|---|---|---|
| Hiring +25% | 995 | 1,030 | −35 | −30 |
| Retention program (attrition −25%) | 991 | 1,030 | −39 | −41 |
| Current trend | 985 | 1,030 | −45 | −58 |
| Hiring freeze | 938 | 1,030 | −92 | −173 |

**Two of sixteen objectives are on track one third of the way through the quarter.** Seven are off track, six at risk, and one cannot be assessed because its key results have not been updated ([02](outputs/02_okr_progress_and_pacing__q1.csv)). Of 40 key results, 4 are stale.

**The alert feed recovers every seeded anomaly and nothing else.** Five anomalies were planted in 3,851 daily KPI readings; the seasonality-aware detector flags exactly those five, plus the one missing day ([03](outputs/)). A plain trailing-28-day z-score on the same data scores the two ticket-volume anomalies at about half the strength (6.4 and −5.4 instead of 13.9 and −10.3), because the weekly rhythm inflates the spread, and at the textbook 3-sigma threshold it adds four false alerts.

**The scorecard says which numbers to caveat.** 14 duplicated event records, 3 events for an unknown org, 4 terminations dated before hire, 94 events recorded more than ten days late, one objective with weights summing to 1.2 ([05](outputs/05_data_quality_checks__q1.csv)). Each has a stated effect on a specific report and a stated handling.

**[→ BRIEF.md](BRIEF.md)** turns these into the one-page readout a leadership team would actually receive.

---

## How the results were checked

- **Independent recomputation.** Month-end headcount for all eight orgs at 2026-10-31 was rebuilt in plain Python from the raw event CSV, with no SQL, and matches the query output for every org.
- **Known-answer tests.** The generator plants five KPI anomalies, one missing day, one bad OKR weighting, three unknown-org events and four impossible terminations. The alert feed and scorecard find exactly those counts.
- **Totals reconcile.** Org rows in the plan-vs-actual output sum to the organization totals quoted above; scenario rows sum to the scenario totals.

## The data

Seeded (`SEED = 20261031`), regenerated by one command, about 280 KB.

| Table | Grain | Rows |
|---|---|---|
| `orgs` | one per org (8 orgs in 3 functions) | 8 |
| `workforce_events` | one per recorded hire, transfer or termination | 1,575 |
| `headcount_plan` | one per org per quarter end | 80 |
| `okr_key_results` | one per key result, current quarter | 40 |
| `okr_checkins` | one per key-result check-in | 159 |
| `kpi_daily` | one per org, metric and day | 3,851 |

## Honest scope

- The scenario model uses constant monthly rates with no seasonality or hiring lead time. It sizes a gap and compares options; it is not a forecast.
- Pacing bands and the alert threshold are stated parameters, chosen for this demo.
- Local execution proves the logic. It says nothing about BigQuery cost or performance at production volume; the delivery project's [PERFORMANCE.md](../enterprise-delivery-analytics/PERFORMANCE.md) covers that separately.
