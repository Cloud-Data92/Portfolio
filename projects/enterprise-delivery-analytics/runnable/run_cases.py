"""
Run the BigQuery case studies in ../sql/ locally and write their results.

The .sql files are written in BigQuery Standard SQL. This script loads the
synthetic CSVs from data/ into an in-memory DuckDB database, translates each
statement from BigQuery to DuckDB with sqlglot, runs it and writes the result
to outputs/. Nothing is rewritten by hand, so the committed outputs are the
results of the SQL exactly as published.

    python3 generate_events.py
    pip install duckdb sqlglot          # or: uv run --with duckdb --with sqlglot run_cases.py
    python3 run_cases.py                # all runnable case studies
    python3 run_cases.py 12             # just case study 12
"""

import csv
import sys
from pathlib import Path

import duckdb
import sqlglot

HERE = Path(__file__).parent
SQL_DIR = HERE.parent / "sql"
RUNNABLE = ["11_terminal_outcome_precedence", "12_event_vs_ingestion_latency",
            "13_pickup_dwell_time_windows"]


def connect():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA portfolio_demo")
    for table in ["stores", "carriers", "deliveries", "delivery_status_events"]:
        path = HERE / "data" / f"{table}.csv"
        if not path.exists():
            sys.exit("data/ is empty - run generate_events.py first")
        con.execute(
            f"CREATE TABLE portfolio_demo.{table} AS "
            f"SELECT * FROM read_csv('{path}', header = true)"
        )
        # BigQuery TIMESTAMP is an absolute instant; DuckDB's equivalent is TIMESTAMPTZ
        for (column,) in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'portfolio_demo' AND table_name = ? AND column_name LIKE '%_ts'",
            [table],
        ).fetchall():
            con.execute(f"ALTER TABLE portfolio_demo.{table} ALTER {column} TYPE TIMESTAMPTZ")
    return con


def show(columns, rows, limit=8):
    table = [columns] + [[("" if v is None else str(v)) for v in r] for r in rows[:limit]]
    widths = [max(len(r[i]) for r in table) for i in range(len(columns))]
    for n, r in enumerate(table):
        print("    " + "  ".join(c.ljust(w) for c, w in zip(r, widths)))
        if n == 0:
            print("    " + "  ".join("-" * w for w in widths))
    if len(rows) > limit:
        print(f"    ... {len(rows) - limit} more rows")


def main():
    wanted = [n for n in RUNNABLE if not sys.argv[1:] or n.split("_")[0] in sys.argv[1:]]
    con = connect()
    (HERE / "outputs").mkdir(exist_ok=True)
    for name in wanted:
        parsed = sqlglot.parse((SQL_DIR / f"{name}.sql").read_text(), read="bigquery")
        # a trailing comment block parses as an empty statement - skip it
        statements = [e.sql(dialect="duckdb") for e in parsed
                      if e is not None and e.find(sqlglot.exp.Select)]
        for i, statement in enumerate(statements, start=1):
            cur = con.execute(statement)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
            out = HERE / "outputs" / f"{name}__q{i}.csv"
            with open(out, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(columns)
                w.writerows(rows)
            print(f"\n{name}  query {i}  ->  outputs/{out.name}  ({len(rows)} rows)")
            show(columns, rows)


if __name__ == "__main__":
    main()
