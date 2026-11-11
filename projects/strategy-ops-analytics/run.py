"""
Run the BigQuery SQL in sql/ locally and write each result to outputs/.

The .sql files are BigQuery Standard SQL and are the source of truth. This
script loads data/*.csv into an in-memory DuckDB database, translates each
statement from BigQuery to DuckDB with sqlglot, and runs the files in order
(01 builds two curated tables that 04 reads). No query is edited by hand.

    python3 generate_data.py
    uv run --with duckdb --with sqlglot --with pytz python run.py     # or pip install the three
"""

import csv
import sys
from pathlib import Path

import duckdb
import sqlglot
from sqlglot import exp

HERE = Path(__file__).parent


def connect():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA stratops_demo")
    files = sorted((HERE / "data").glob("*.csv"))
    if not files:
        sys.exit("data/ is empty - run generate_data.py first")
    for path in files:
        con.execute(f"CREATE TABLE stratops_demo.{path.stem} AS "
                    f"SELECT * FROM read_csv('{path}', header = true)")
    return con


def show(columns, rows, limit=10):
    table = [columns] + [["" if v is None else str(v) for v in r] for r in rows[:limit]]
    widths = [max(len(r[i]) for r in table) for i in range(len(columns))]
    for n, r in enumerate(table):
        print("    " + "  ".join(c.ljust(w) for c, w in zip(r, widths)))
        if n == 0:
            print("    " + "  ".join("-" * w for w in widths))
    if len(rows) > limit:
        print(f"    ... {len(rows) - limit} more rows")


def main():
    con = connect()
    (HERE / "outputs").mkdir(exist_ok=True)
    for path in sorted((HERE / "sql").glob("*.sql")):
        q = 0
        for e in sqlglot.parse(path.read_text(), read="bigquery"):
            if e is None:
                continue                                   # trailing comment block
            statement = e.sql(dialect="duckdb")
            if isinstance(e, exp.Create):
                con.execute(statement)
                print(f"\n{path.stem}: built {e.this.sql(dialect='duckdb')}")
                continue
            if not e.find(exp.Select):
                continue
            q += 1
            cur = con.execute(statement)
            columns = [d[0] for d in cur.description]
            # BigQuery returns DATE where DuckDB returns a midnight TIMESTAMP
            rows = [tuple(v.date() if hasattr(v, "hour") and (v.hour, v.minute, v.second) == (0, 0, 0)
                          else v for v in r) for r in cur.fetchall()]
            out = HERE / "outputs" / f"{path.stem}__q{q}.csv"
            with open(out, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(columns)
                w.writerows(rows)
            print(f"\n{path.stem}  query {q}  ->  outputs/{out.name}  ({len(rows)} rows)")
            show(columns, rows)


if __name__ == "__main__":
    main()
