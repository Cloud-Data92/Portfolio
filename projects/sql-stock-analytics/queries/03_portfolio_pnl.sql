-- ============================================================
-- Query 3: Portfolio Profit & Loss by Position
-- Skills shown: multi-CTE pipelines, conditional aggregation,
--               joins across 3 tables, derived metrics
-- ============================================================
-- From the raw trade log, compute per-ticker: current shares
-- held, total invested, realized proceeds, average cost basis,
-- current market value, and unrealized P&L vs the latest close.

WITH position AS (
    SELECT
        ticker,
        -- signed share count: buys add, sells subtract
        SUM(CASE WHEN side = 'BUY'  THEN quantity ELSE -quantity END) AS shares_held,
        SUM(CASE WHEN side = 'BUY'  THEN quantity * fill_price ELSE 0 END) AS total_bought,
        SUM(CASE WHEN side = 'SELL' THEN quantity * fill_price ELSE 0 END) AS total_sold,
        SUM(CASE WHEN side = 'BUY'  THEN quantity ELSE 0 END)             AS shares_bought
    FROM trades
    GROUP BY ticker
),
latest AS (
    -- most recent close per ticker
    SELECT ticker, close AS latest_close
    FROM daily_prices AS p
    WHERE trade_date = (SELECT MAX(trade_date)
                        FROM daily_prices
                        WHERE ticker = p.ticker)
)
SELECT
    pos.ticker,
    c.company_name,
    c.sector,
    pos.shares_held,
    ROUND(pos.total_bought / pos.shares_bought, 2)        AS avg_cost_per_share,
    ROUND(l.latest_close, 2)                              AS latest_close,
    ROUND(pos.shares_held * l.latest_close, 2)            AS market_value,
    -- unrealized P&L = current value minus cost basis of remaining shares
    ROUND(pos.shares_held * (l.latest_close
          - pos.total_bought / pos.shares_bought), 2)     AS unrealized_pnl,
    ROUND(100.0 * (l.latest_close
          - pos.total_bought / pos.shares_bought)
          / (pos.total_bought / pos.shares_bought), 1)    AS unrealized_pnl_pct
FROM position AS pos
JOIN companies AS c ON c.ticker = pos.ticker
JOIN latest    AS l ON l.ticker = pos.ticker
WHERE pos.shares_held > 0
ORDER BY unrealized_pnl DESC;
