"""
ETL pipeline: messy raw sales CSV  →  validated, deduplicated  →  star-schema warehouse.

Extract   read data/raw_sales.csv
Transform normalize dates (3 formats), prices ($, whitespace), store casing;
          validate every row; quarantine bad rows to outputs/rejects.csv with reasons
Load      upsert dimensions, insert facts idempotently (re-running never duplicates)

Run: python3 make_raw_data.py && python3 etl.py
Test: python3 -m unittest test_etl.py
"""

import csv
import sqlite3
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).parent
DB_PATH = ROOT / "warehouse.db"

DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y")


# ── Transform helpers (pure functions — unit tested in test_etl.py) ──────────

def parse_date(raw: str) -> date:
    """Accept the three date formats the feed is known to send."""
    raw = raw.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unparseable date: {raw!r}")


def parse_price(raw: str) -> float:
    """Strip currency symbols/whitespace; must be a positive number."""
    value = float(raw.strip().lstrip("$"))
    if value <= 0:
        raise ValueError(f"non-positive price: {raw!r}")
    return round(value, 2)


def parse_quantity(raw) -> int:
    value = int(str(raw).strip())
    if value <= 0:
        raise ValueError(f"non-positive quantity: {raw!r}")
    return value


def normalize_store(raw: str) -> str:
    """'ATLANTA' / 'atlanta' / ' Atlanta ' → 'Atlanta'."""
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError("missing store")
    return cleaned.title()


def transform_row(row: dict) -> dict:
    """Validate + normalize one raw row. Raises ValueError with a reason on bad data."""
    if not row["sku"].strip():
        raise ValueError("missing sku")
    return {
        "order_id": int(row["order_id"]),
        "order_date": parse_date(row["order_date"]),
        "store": normalize_store(row["store"]),
        "sku": row["sku"].strip(),
        "product_name": row["product_name"].strip(),
        "category": row["category"].strip(),
        "quantity": parse_quantity(row["quantity"]),
        "unit_price": parse_price(row["unit_price"]),
    }


# ── Load helpers ──────────────────────────────────────────────────────────────

def date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def ensure_date(conn, d: date) -> int:
    key = date_key(d)
    conn.execute(
        """INSERT OR IGNORE INTO dim_date
           (date_key, full_date, year, quarter, month, month_name, day_of_week, is_weekend)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (key, d.isoformat(), d.year, (d.month - 1) // 3 + 1, d.month,
         d.strftime("%B"), d.strftime("%A"), 1 if d.weekday() >= 5 else 0),
    )
    return key


def ensure_store(conn, name: str) -> int:
    conn.execute("INSERT OR IGNORE INTO dim_store (store_name) VALUES (?)", (name,))
    return conn.execute(
        "SELECT store_key FROM dim_store WHERE store_name = ?", (name,)
    ).fetchone()[0]


def ensure_product(conn, sku: str, name: str, category: str) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO dim_product (sku, product_name, category) VALUES (?, ?, ?)",
        (sku, name, category),
    )
    return conn.execute(
        "SELECT product_key FROM dim_product WHERE sku = ?", (sku,)
    ).fetchone()[0]


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript((ROOT / "warehouse_schema.sql").read_text())

    outputs = ROOT / "outputs"
    outputs.mkdir(exist_ok=True)

    stats = {"read": 0, "loaded": 0, "duplicates": 0, "rejected": 0}
    rejects = []
    seen_orders = set()

    with open(ROOT / "data" / "raw_sales.csv", newline="") as f:
        for raw in csv.DictReader(f):
            stats["read"] += 1
            try:
                row = transform_row(raw)
            except (ValueError, KeyError) as e:
                stats["rejected"] += 1
                rejects.append({**raw, "reject_reason": str(e)})
                continue

            if row["order_id"] in seen_orders:
                stats["duplicates"] += 1
                continue
            seen_orders.add(row["order_id"])

            dk = ensure_date(conn, row["order_date"])
            sk = ensure_store(conn, row["store"])
            pk = ensure_product(conn, row["sku"], row["product_name"], row["category"])
            conn.execute(
                """INSERT OR IGNORE INTO fact_sales
                   (order_id, date_key, store_key, product_key, quantity, unit_price, revenue)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (row["order_id"], dk, sk, pk, row["quantity"], row["unit_price"],
                 round(row["quantity"] * row["unit_price"], 2)),
            )
            stats["loaded"] += 1

    conn.commit()

    with open(outputs / "rejects.csv", "w", newline="") as f:
        if rejects:
            w = csv.DictWriter(f, fieldnames=list(rejects[0].keys()))
            w.writeheader()
            w.writerows(rejects)

    print("ETL load summary")
    for k, v in stats.items():
        print(f"  {k:<12}{v}")

    # data-quality gate: fail loudly if the reject rate is abnormal
    reject_rate = stats["rejected"] / stats["read"]
    assert reject_rate < 0.10, f"reject rate {reject_rate:.1%} exceeds 10% threshold"

    # sample analytical query against the star schema
    print("\nMonthly revenue by category (top 12 rows):")
    cur = conn.execute("""
        SELECT d.year, d.month_name, p.category,
               ROUND(SUM(f.revenue), 2) AS revenue,
               SUM(f.quantity)          AS units
        FROM fact_sales AS f
        JOIN dim_date    AS d ON d.date_key    = f.date_key
        JOIN dim_product AS p ON p.product_key = f.product_key
        GROUP BY d.year, d.month, p.category
        ORDER BY d.year, d.month, revenue DESC
    """)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    with open(outputs / "monthly_revenue_by_category.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print("  " + " | ".join(cols))
    for r in rows[:12]:
        print("  " + " | ".join(str(v) for v in r))
    print(f"  ... full results in outputs/monthly_revenue_by_category.csv ({len(rows)} rows)")

    conn.close()


if __name__ == "__main__":
    run()
