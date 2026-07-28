# Query Cost & Performance Engineering

On a consumption-priced warehouse, a query's cost is a design output, not an accident. This document works through the cost and performance reasoning behind the case studies — the part of SQL that separates "returns the right rows" from "runs every hour for three years without anyone noticing."

All figures below are worked estimates against the synthetic schema, using BigQuery's public on-demand pricing model ($6.25/TiB scanned as of writing). They demonstrate the method; substitute your own table statistics.

---

## 1. Scanned bytes are the unit of cost

BigQuery on-demand pricing bills **bytes scanned**, not rows returned or time elapsed. Three consequences drive most of the decisions in these queries:

| Practice | Why it matters |
|---|---|
| Never `SELECT *` on a wide fact table | Columnar storage means unread columns cost nothing — reading all of them costs everything |
| Filter on the partition column | Partition pruning is the single largest lever available |
| Cluster on real filter predicates | Block pruning within partitions, roughly free at write time |

### Worked example — partition pruning

Take the curated order-detail fact from [case study 10](sql/10_incremental_merge_architecture.sql): ~6M rows/year, 40 columns, ~4 KiB/row uncompressed → **~24 GiB/year**.

A dashboard query reading 6 columns over a 30-day window:

```
Unpartitioned, SELECT *          : 24 GiB scanned    ≈ $0.15/query
Unpartitioned, 6 columns         :  3.6 GiB scanned  ≈ $0.022/query
Partitioned by business_date,
  30-day filter, 6 columns       :  0.30 GiB scanned ≈ $0.0018/query
```

**~80× reduction.** At a 15-minute refresh cadence (35,040 runs/year) that is the difference between roughly $5,300/year and $65/year for one dashboard's backing query. This is why `PARTITION BY business_date CLUSTER BY market_id, carrier_id, store_id` appears in the table definition rather than being left to chance.

The trap: partition pruning only fires when the filter is a **literal or a constant-folded expression** on the partition column. `WHERE business_date >= CURRENT_DATE() - 30` prunes; `WHERE CAST(business_date AS STRING) LIKE '2026-%'` does not, and silently scans everything.

---

## 2. Prune the candidate space before the expensive operation

The geospatial work in [case study 3](sql/03_geospatial_store_serviceability.sql) is the clearest instance. A naive nearest-store query is a cross join: every ZIP against every store.

```
2,000 stores × 41,000 US ZIPs = 82,000,000 distance computations
```

`ST_DISTANCE` on a geography pair is far from free, and the intermediate result may spill to shuffle. Two filters cut this dramatically:

| Applied filter | Candidate pairs | Reduction |
|---|---|---|
| None (raw cross join) | 82,000,000 | — |
| Same-market join predicate | ~1,600,000 | ~51× |
| `+ ST_DWITHIN` 300-mile bound | ~180,000 | ~455× |

`ST_DWITHIN` matters more than it looks: BigQuery can service it from the **S2 cell coverings** in its spatial index rather than computing exact geodesic distance, so it discards the overwhelming majority of pairs before any precise math runs. The exact `ST_DISTANCE` then evaluates only on survivors.

The same principle drives the ordering of the `ROW_NUMBER()` window — ranking a pruned set of ~90 candidates per origin is trivial; ranking 41,000 is a sort that spills.

> **On the ZIP-coverage variant:** it is written with an explicit `CROSS JOIN` for readability, and the file says so. In a real deployment I would gate it behind a market or bounding-box join first, and materialize the result — ZIP-to-store assignment changes when stores open or close, not hourly, so recomputing it per dashboard load is pure waste. Recognizing which results are *stable reference data* versus *live metrics* is itself a cost decision.

---

## 3. Complexity of the analytical patterns

Reviewers reasonably ask what these queries cost as data grows. Per partition scanned, with *n* input rows:

| Pattern | Complexity | Scaling note |
|---|---|---|
| `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)` (cases 5, 9, 10) | O(n log n) | Sort-bound; parallelizes across partition keys. Skewed keys are the failure mode — one hot partition serializes the stage |
| `PERCENT_RANK()` over the full window (case 8) | O(n log n) | Global ordering, no partition key → single-node sort risk. Safe here because the input is pre-aggregated to market-week grain (hundreds of rows), *not* raw facts. Aggregating before ranking is the design decision |
| Conditional aggregation / `COUNT(DISTINCT …)` (cases 4, 7) | O(n) with sketch-based distinct | BigQuery's approximate path is default for large cardinalities; `COUNT(DISTINCT)` here is exact because the per-group cardinality is small |
| Rolling frame `ROWS BETWEEN 4 PRECEDING …` (case 8) | O(n) | Bounded frames stream; `RANGE`-based frames on timestamps can degrade |
| `MERGE` with lookback (case 10) | O(m log m + matched writes) | *m* = lookback slice, not table size — this is the entire point of the incremental design |

The recurring theme: **shrink the input before the superlinear step.** Case 8 aggregates to market-week before the global `PERCENT_RANK`; case 3 prunes spatially before ranking; case 10 merges a two-day slice instead of a full table.

---

## 4. Incremental loads over full refreshes

[Case study 10](sql/10_incremental_merge_architecture.sql) reloads a 2-day lookback window rather than rebuilding the table:

```
Full refresh:        24 GiB scanned + 24 GiB written, per run
Incremental MERGE:   ~130 MiB scanned + only changed rows written
```

Roughly **180× less scanned per run**, and the runtime drops from minutes to seconds — which in turn means the refresh can be frequent enough to matter operationally. The two-day lookback is the tuning knob: wide enough to absorb late-arriving corrections, narrow enough to stay cheap. Choosing it requires knowing the actual distribution of source-event lateness, which is a measurement, not a guess.

The `QUALIFY ROW_NUMBER() … = 1` inside the `USING` clause is a correctness requirement, not an optimization: without it, duplicate source events for the same key make the `MERGE` non-deterministic and BigQuery raises an error rather than silently picking one.

---

## 5. Serving-layer separation

Splitting the KPI layer from the order-detail layer is a performance decision expressed as architecture:

| Layer | Rows | Refresh | Consumer |
|---|---|---|---|
| Pre-aggregated KPI fact | ~10³–10⁴ | Frequent, cheap | Executive dashboards, heatmaps |
| Curated order detail | ~10⁶–10⁷ | Incremental | Drill-through, investigation |

Two products with different service levels, sharing conformed dimensions. The alternative — one extract serving both — forces the aggregate consumers to pay detail-layer refresh costs, and the common failure mode is a BI extract that grows until it can no longer refresh inside its window. Pushing aggregation upstream into the warehouse, where it is cheap and cached, rather than into the BI tool, where it is neither, is the fix.

---

## 6. What I would measure in a real deployment

Estimates are a starting point; the numbers above are reasoning, not evidence. On a live system I would instrument:

- `INFORMATION_SCHEMA.JOBS` — bytes scanned, slot-milliseconds, and cost attributed **per query per day**, trended
- Partition-pruning verification via dry-run byte estimates in CI, so a regression that removes pruning fails a check instead of arriving as an invoice
- Slot contention during overlapping scheduled windows — the reason staggered schedules exist
- Source-event lateness distribution, which sets the lookback window empirically
- Cache hit rates, since repeated identical dashboard queries should be free

The general principle: **cost and latency are correctness properties.** A query that returns the right answer too slowly, too expensively, or too late for the decision it informs has not actually solved the problem.
