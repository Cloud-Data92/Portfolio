-- ============================================================
-- Query 4: Monthly Returns with Rankings & Volatility
-- Skills shown: date bucketing (strftime), window functions
--               (FIRST_VALUE, LAST_VALUE, RANK), nested CTEs,
--               statistical aggregates
-- ============================================================
-- For every ticker and month: the month's % return and how that
-- ticker RANKED against all others that month. The final SELECT
-- keeps each month's top-3 performers — a leaderboard over time.

WITH monthly AS (
    SELECT
        ticker,
        strftime('%Y-%m', trade_date) AS month,
        FIRST_VALUE(open) OVER (
            PARTITION BY ticker, strftime('%Y-%m', trade_date)
            ORDER BY trade_date
        ) AS month_open,
        LAST_VALUE(close) OVER (
            PARTITION BY ticker, strftime('%Y-%m', trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_close
    FROM daily_prices
),
monthly_returns AS (
    SELECT DISTINCT
        ticker,
        month,
        ROUND(100.0 * (month_close - month_open) / month_open, 2) AS return_pct
    FROM monthly
),
ranked AS (
    SELECT
        month,
        ticker,
        return_pct,
        RANK() OVER (PARTITION BY month ORDER BY return_pct DESC) AS month_rank
    FROM monthly_returns
)
SELECT
    r.month,
    r.month_rank,
    r.ticker,
    c.sector,
    r.return_pct
FROM ranked   AS r
JOIN companies AS c ON c.ticker = r.ticker
WHERE r.month_rank <= 3
ORDER BY r.month, r.month_rank;
