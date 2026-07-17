# 🏗️ ETL Pipeline → Star-Schema Data Warehouse

A production-style ETL pipeline in pure Python (no dependencies): it ingests a deliberately **messy** raw sales feed — duplicate records, three competing date formats, `$`-signs and whitespace in prices, inconsistent store casing, missing SKUs, garbage quantities — and lands clean, validated facts in a Kimball-style **star schema**, with every bad row quarantined and explained.

This mirrors the data warehousing and ETL work I did professionally at UPS and UL Environment, rebuilt from scratch as a runnable showcase.

```bash
python3 make_raw_data.py     # generate the messy source feed (seeded, reproducible)
python3 etl.py               # extract → transform → load, prints the load summary
python3 -m unittest test_etl.py -v   # 18 unit tests on the transform layer
```

## Real output from a real run

```
ETL load summary
  read        3139
  loaded      3000
  duplicates  90
  rejected    49
```

Every rejected row lands in [`outputs/rejects.csv`](outputs/rejects.csv) **with its reject reason** (`missing sku`, `non-positive quantity: '-2'`, …) — bad data is never silently dropped, and a data-quality gate fails the whole load if the reject rate exceeds 10%.

## The warehouse design (star schema)

```
                 dim_date (date_key, year, quarter, month, day_of_week, is_weekend)
                     ▲
dim_store ◄── fact_sales (order_id, quantity, unit_price, revenue) ──► dim_product
(store_key,        one row per valid order                    (product_key, sku,
 store_name)                                                   name, category)
```

Why this shape ([`warehouse_schema.sql`](warehouse_schema.sql)):

- **Surrogate keys** on dimensions; the fact table stores compact integer keys
- **`dim_date`** precomputes year/quarter/month/weekday — BI tools group by these constantly
- **`UNIQUE(order_id)`** on the fact table + `INSERT OR IGNORE` makes loads **idempotent**: re-running the pipeline can never double-count
- Precomputed `revenue` column so Tableau/reporting queries never re-derive it

## Engineering practices demonstrated

| Practice | Where |
|---|---|
| Pure, unit-testable transform functions | [`etl.py`](etl.py) top section, tested in [`test_etl.py`](test_etl.py) |
| Reject/quarantine pattern with reasons | `outputs/rejects.csv` |
| Data-quality gate (fail loud, not silent) | reject-rate assertion in `etl.py` |
| Idempotent loads | natural-key uniqueness + upsert-style dimension loading |
| Dimensional modeling (Kimball star schema) | `warehouse_schema.sql` |

The final analytical query (monthly revenue by category, produced from the star schema) is committed at [`outputs/monthly_revenue_by_category.csv`](outputs/monthly_revenue_by_category.csv) — Tableau-ready.
