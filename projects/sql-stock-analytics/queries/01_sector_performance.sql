-- ============================================================
-- Query 1: Sector Performance Summary
-- Skills shown: JOIN, GROUP BY, aggregate functions,
--               correlated scalar subqueries, ROUND/formatting
-- ============================================================
-- For each sector: number of companies, average daily close,
-- total volume traded, and the sector's overall % return from
-- the first to the last trading day in the dataset.

WITH first_last AS (
    SELECT
        p.ticker,
        -- close on the earliest date for this ticker
        (SELECT close FROM daily_prices
          WHERE ticker = p.ticker
          ORDER BY trade_date ASC  LIMIT 1) AS first_close,
        -- close on the latest date for this ticker
        (SELECT close FROM daily_prices
          WHERE ticker = p.ticker
          ORDER BY trade_date DESC LIMIT 1) AS last_close
    FROM daily_prices AS p
    GROUP BY p.ticker
)
SELECT
    c.sector,
    COUNT(DISTINCT c.ticker)                                   AS companies,
    ROUND(AVG(fl.last_close), 2)                               AS avg_latest_close,
    ROUND(AVG((fl.last_close - fl.first_close)
              / fl.first_close) * 100, 1)                      AS avg_total_return_pct
FROM companies    AS c
JOIN first_last   AS fl ON fl.ticker = c.ticker
GROUP BY c.sector
ORDER BY avg_total_return_pct DESC;
