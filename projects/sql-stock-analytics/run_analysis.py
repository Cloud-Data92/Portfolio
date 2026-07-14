"""
Build the SQLite database from the CSVs and run every query in queries/.

Results are printed to the terminal and written to outputs/<query>.csv
(so the repo shows real results, and the CSVs are Tableau-ready).

Run: python3 run_analysis.py
"""

import csv
import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent
DB_PATH = ROOT / "market.db"


def load_csv(conn: sqlite3.Connection, table: str, path: Path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        placeholders = ",".join("?" * len(header))
        conn.executemany(
            f"INSERT INTO {table} ({','.join(header)}) VALUES ({placeholders})",
            reader,
        )


def main():
    DB_PATH.unlink(missing_ok=True)
    conn = sqlite3.connect(DB_PATH)

    conn.executescript((ROOT / "schema.sql").read_text())
    for table in ("companies", "daily_prices", "trades"):
        load_csv(conn, table, ROOT / "data" / f"{table}.csv")
    conn.commit()

    outputs = ROOT / "outputs"
    outputs.mkdir(exist_ok=True)

    for sql_file in sorted((ROOT / "queries").glob("*.sql")):
        print(f"\n{'=' * 70}\n{sql_file.name}\n{'=' * 70}")
        cur = conn.execute(sql_file.read_text())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        out_path = outputs / f"{sql_file.stem}.csv"
        with open(out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)

        # print a small preview
        print(" | ".join(cols))
        for row in rows[:10]:
            print(" | ".join(str(v) for v in row))
        if len(rows) > 10:
            print(f"... ({len(rows)} rows total, full results in outputs/{out_path.name})")

    conn.close()
    print(f"\nDatabase written to {DB_PATH.name}, results in outputs/")


if __name__ == "__main__":
    main()
