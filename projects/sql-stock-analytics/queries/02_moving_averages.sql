-- ============================================================
-- Query 2: 50-day vs 200-day Moving Averages (Golden Cross)
-- Skills shown: window functions (AVG OVER with ROWS frame),
--               CTEs, LAG, CASE, date filtering
-- ============================================================
-- Classic technical-analysis signal: when the 50-day moving
-- average crosses above the 200-day ("golden cross") it is a
-- bullish signal; crossing below ("death cross") is bearish.
-- This finds every crossover event in the dataset.

WITH ma AS (
    SELECT
        ticker,
        trade_date,
        close,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma50,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS ma200,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date) AS rn
    FROM daily_prices
),
signals AS (
    SELECT
        ticker,
        trade_date,
        close,
        ma50,
        ma200,
        -- compare today's relationship to yesterday's to detect a cross
        CASE WHEN ma50 > ma200 THEN 1 ELSE 0 END AS above,
        LAG(CASE WHEN ma50 > ma200 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_above
    FROM ma
    WHERE rn >= 200          -- only once the 200-day window is fully formed
)
SELECT
    ticker,
    trade_date,
    ROUND(close, 2)  AS close,
    ROUND(ma50, 2)   AS ma50,
    ROUND(ma200, 2)  AS ma200,
    CASE WHEN above = 1 THEN 'GOLDEN CROSS (bullish)'
         ELSE              'DEATH CROSS (bearish)' END AS signal
FROM signals
WHERE above <> prev_above    -- the day the cross happened
ORDER BY trade_date, ticker;
