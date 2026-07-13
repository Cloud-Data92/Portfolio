"""
Generate a realistic, reproducible stock-market dataset for the SQL analytics project.

Produces three CSVs in data/:
  - companies.csv      one row per ticker (sector, name)
  - daily_prices.csv   3 years of simulated daily OHLCV per ticker (geometric random walk)
  - trades.csv         a simulated personal portfolio trade log (buys/sells)

Deterministic: seeded RNG so anyone cloning the repo gets identical data.
Run: python3 generate_data.py
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

COMPANIES = [
    ("AAPL", "Apple Inc.", "Technology", 185.0, 0.00045, 0.016),
    ("MSFT", "Microsoft Corp.", "Technology", 370.0, 0.00050, 0.015),
    ("NVDA", "NVIDIA Corp.", "Technology", 480.0, 0.00110, 0.030),
    ("GOOG", "Alphabet Inc.", "Technology", 140.0, 0.00040, 0.017),
    ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", 150.0, 0.00042, 0.020),
    ("TSLA", "Tesla Inc.", "Consumer Discretionary", 240.0, 0.00030, 0.035),
    ("HD", "Home Depot Inc.", "Consumer Discretionary", 330.0, 0.00025, 0.013),
    ("JPM", "JPMorgan Chase & Co.", "Financials", 155.0, 0.00035, 0.014),
    ("BAC", "Bank of America Corp.", "Financials", 32.0, 0.00020, 0.016),
    ("V", "Visa Inc.", "Financials", 250.0, 0.00038, 0.012),
    ("JNJ", "Johnson & Johnson", "Healthcare", 158.0, 0.00015, 0.010),
    ("UNH", "UnitedHealth Group", "Healthcare", 520.0, 0.00030, 0.013),
    ("PFE", "Pfizer Inc.", "Healthcare", 29.0, -0.00010, 0.015),
    ("XOM", "Exxon Mobil Corp.", "Energy", 105.0, 0.00022, 0.017),
    ("CVX", "Chevron Corp.", "Energy", 150.0, 0.00018, 0.016),
    ("PG", "Procter & Gamble Co.", "Consumer Staples", 152.0, 0.00018, 0.009),
    ("KO", "Coca-Cola Co.", "Consumer Staples", 59.0, 0.00014, 0.008),
    ("WMT", "Walmart Inc.", "Consumer Staples", 160.0, 0.00028, 0.010),
    ("BA", "Boeing Co.", "Industrials", 210.0, 0.00005, 0.024),
    ("CAT", "Caterpillar Inc.", "Industrials", 280.0, 0.00032, 0.016),
]

START = date(2023, 1, 2)
END = date(2025, 12, 31)


def trading_days(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri; holidays ignored for simplicity
            yield d
        d += timedelta(days=1)


def simulate_prices():
    days = list(trading_days(START, END))
    rows = []
    for ticker, _name, _sector, start_price, drift, vol in COMPANIES:
        price = start_price
        for d in days:
            daily_ret = random.gauss(drift, vol)
            open_p = price
            close_p = max(1.0, price * (1 + daily_ret))
            high_p = max(open_p, close_p) * (1 + abs(random.gauss(0, vol / 3)))
            low_p = min(open_p, close_p) * (1 - abs(random.gauss(0, vol / 3)))
            volume = int(random.lognormvariate(15.5, 0.6))
            rows.append(
                (ticker, d.isoformat(), round(open_p, 2), round(high_p, 2),
                 round(low_p, 2), round(close_p, 2), volume)
            )
            price = close_p
    return days, rows


def simulate_trades(days, price_lookup):
    """A believable personal trade log: periodic buys, occasional sells."""
    rows = []
    trade_id = 1
    tickers = [c[0] for c in COMPANIES]
    holdings = {t: 0 for t in tickers}
    for d in days:
        # ~2 trades a week on average
        if random.random() < 0.4:
            for _ in range(random.choice([1, 1, 2])):
                ticker = random.choice(tickers)
                close = price_lookup[(ticker, d.isoformat())]
                # sell only if we hold shares; mostly buys (accumulating portfolio)
                if holdings[ticker] > 0 and random.random() < 0.3:
                    side = "SELL"
                    qty = random.randint(1, holdings[ticker])
                    holdings[ticker] -= qty
                else:
                    side = "BUY"
                    qty = random.randint(1, 15)
                    holdings[ticker] += qty
                # fill near the close with a little slippage
                fill = round(close * random.uniform(0.995, 1.005), 2)
                rows.append((trade_id, d.isoformat(), ticker, side, qty, fill))
                trade_id += 1
    return rows


def main():
    days, price_rows = simulate_prices()
    price_lookup = {(r[0], r[1]): r[5] for r in price_rows}
    trade_rows = simulate_trades(days, price_lookup)

    with open(DATA_DIR / "companies.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "company_name", "sector"])
        w.writerows([(t, n, s) for t, n, s, *_ in COMPANIES])

    with open(DATA_DIR / "daily_prices.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "trade_date", "open", "high", "low", "close", "volume"])
        w.writerows(price_rows)

    with open(DATA_DIR / "trades.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trade_id", "trade_date", "ticker", "side", "quantity", "fill_price"])
        w.writerows(trade_rows)

    print(f"companies.csv:    {len(COMPANIES)} rows")
    print(f"daily_prices.csv: {len(price_rows)} rows")
    print(f"trades.csv:       {len(trade_rows)} rows")


if __name__ == "__main__":
    main()
