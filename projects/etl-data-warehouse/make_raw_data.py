"""
Generate intentionally MESSY raw retail sales data — the kind of file a real
ETL pipeline receives: duplicate rows, mixed date formats, dollar signs in
price fields, inconsistent casing, missing values, and bad records.

Deterministic (seeded) so results are reproducible.
Run: python3 make_raw_data.py
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(7)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

STORES = ["Atlanta", "Marietta", "Kennesaw", "Alpharetta", "Smyrna"]
PRODUCTS = [
    ("SKU-1001", "Wireless Mouse", "Electronics", 24.99),
    ("SKU-1002", "Mechanical Keyboard", "Electronics", 89.99),
    ("SKU-1003", "USB-C Hub", "Electronics", 39.99),
    ("SKU-2001", "Office Chair", "Furniture", 189.00),
    ("SKU-2002", "Standing Desk", "Furniture", 349.00),
    ("SKU-3001", "Notebook 3-Pack", "Office Supplies", 8.49),
    ("SKU-3002", "Gel Pens 12ct", "Office Supplies", 11.99),
    ("SKU-3003", "Desk Organizer", "Office Supplies", 21.50),
]


def messy_date(d: date) -> str:
    """Real feeds never agree on a date format."""
    fmt = random.random()
    if fmt < 0.5:
        return d.isoformat()                       # 2025-03-14
    if fmt < 0.8:
        return d.strftime("%m/%d/%Y")              # 03/14/2025
    return d.strftime("%d-%b-%Y")                  # 14-Mar-2025


def messy_price(p: float) -> str:
    r = random.random()
    if r < 0.6:
        return f"{p:.2f}"
    if r < 0.9:
        return f"${p:.2f}"                         # dollar sign sneaks in
    return f" {p:.2f} "                            # stray whitespace


def messy_store(s: str) -> str:
    r = random.random()
    if r < 0.7:
        return s
    if r < 0.85:
        return s.upper()                           # ATLANTA
    return s.lower()                               # atlanta


def main():
    start = date(2025, 1, 1)
    rows = []
    order_id = 50000
    for _ in range(3000):
        d = start + timedelta(days=random.randint(0, 180))
        sku, name, category, price = random.choice(PRODUCTS)
        qty = random.randint(1, 6)
        row = [
            order_id,
            messy_date(d),
            messy_store(random.choice(STORES)),
            sku,
            name,
            category,
            qty,
            messy_price(price),
        ]
        rows.append(row)

        # ~3% exact duplicates (double-sent records)
        if random.random() < 0.03:
            rows.append(list(row))

        # ~2% corrupt records: missing SKU or garbage quantity
        if random.random() < 0.02:
            bad = list(row)
            bad[0] = order_id + 90000
            if random.random() < 0.5:
                bad[3] = ""                        # missing SKU
            else:
                bad[6] = random.choice(["N/A", "-2", ""])  # bad quantity
            rows.append(bad)

        order_id += 1

    random.shuffle(rows)
    out = DATA_DIR / "raw_sales.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "order_date", "store", "sku",
                    "product_name", "category", "quantity", "unit_price"])
        w.writerows(rows)
    print(f"Wrote {len(rows)} raw rows (with intentional dupes/corruption) to {out.name}")


if __name__ == "__main__":
    main()
