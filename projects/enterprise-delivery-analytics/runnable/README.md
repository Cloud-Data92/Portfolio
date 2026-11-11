# Runnable event-data case studies

Case studies [11](../sql/11_terminal_outcome_precedence.sql), [12](../sql/12_event_vs_ingestion_latency.sql) and [13](../sql/13_pickup_dwell_time_windows.sql) are executed, not just written. This folder holds the data generator, the runner, and their committed results. The CSVs themselves (about 7 MB) are not committed; `generate_events.py` rebuilds them byte-for-byte.

```bash
python3 generate_events.py                                   # seeded; standard library only
uv run --with duckdb --with sqlglot python run_cases.py      # or: pip install duckdb sqlglot
python3 run_cases.py 12                                      # a single case study
```

## How the BigQuery SQL runs without BigQuery

The `.sql` files are BigQuery Standard SQL and are the source of truth. `run_cases.py` loads the CSVs into an in-memory DuckDB database, uses [`sqlglot`](https://github.com/tobymao/sqlglot) to translate each statement from the BigQuery dialect to DuckDB's, executes it and writes `outputs/<case>__q<n>.csv`. No query is edited by hand for the local run, so the results are those of the SQL as published. Timestamps load as `TIMESTAMPTZ` with the session pinned to UTC, matching BigQuery's `TIMESTAMP` semantics.

One limit worth stating: this proves the logic, not BigQuery's cost or performance behaviour. Those are covered separately in [PERFORMANCE.md](../PERFORMANCE.md).

## The dataset

Everything is invented and seeded (`SEED = 20260628`), so a fresh clone reproduces every number.

| Table | Grain | Rows |
|---|---|---|
| `stores` | one row per store (`store_id`, `market`, `time_zone`) | 40 |
| `carriers` | one row per carrier | 4 |
| `deliveries` | one row per delivery (`created_ts`, `promised_by_ts`, both UTC) | 15,115 |
| `delivery_status_events` | one row per webhook received (`status`, `event_ts`, `ingested_ts`) | 75,887 |

Status vocabulary: `ASSIGNED`, `ARRIVED_PICKUP`, `DEPARTED_PICKUP`, `ARRIVED_DROPOFF`, and the terminal statuses `COMPLETED`, `MISSED`, `CANCELED`.

## Defects seeded on purpose

| Defect | Why it matters | Exercised by |
|---|---|---|
| ~4% of webhooks are re-sent (same status and `event_ts`, later `ingested_ts`) | Inflates naive counts and averages | 11, 12, 13 |
| Events are stored in arbitrary order | Nothing may depend on row order | all |
| `MISSED` followed by `COMPLETED` (redelivery) | Recency is not the outcome | 11 |
| `CANCELED` both before and after a `MISSED` | The outcome must not depend on which arrived last | 11 |
| Deliveries with no terminal status | Must stay in the denominator as pending | 11 |
| One carrier's webhooks held for 12 hours, then backfilled in a batch | Ingestion time diverges from event time | 12 |
| ~2% stragglers ingested 10–90 minutes late | Long-tailed latency | 12 |
| ~1.5% of pickups have no `DEPARTED_PICKUP` | Dwell cannot be measured; coverage must be reported | 13 |
| Stores in four time zones, all timestamps UTC | Business day and hour are local | 11, 13 |
| Midday dwell is ~40% longer | Only visible when the hour is the store's local hour | 13 |

## How the results were checked

- **Independent recomputation.** The outcome counts for store S032, one of the highest-missed-rate stores in case 11, were recomputed in plain Python straight from the CSVs, without SQL, and match the query output exactly (116 deliveries: 94 completed, 13 missed, 6 canceled, 3 pending).
- **Known-answer defects.** The generator's outage window (Carrier C, 2026-06-12 15:00 to 2026-06-13 03:00 UTC) is recovered by case 12's exception query, and no other carrier-day is flagged.
- **Sanity bounds.** Local arrival hours in case 13 fall only between 08:00 and 19:59, the range the generator creates orders in; a time-zone mistake shows up immediately as pickups at 2 a.m.
- **Totals reconcile.** The four outcome columns in case 11 sum to the delivery count for every store, and the confusion table in query 2 sums to the same 3,460 deliveries.
