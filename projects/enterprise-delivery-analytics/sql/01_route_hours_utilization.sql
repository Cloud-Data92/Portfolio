-- ============================================================================
-- Case Study 1: Route Hours Utilization & Free Capacity
-- Dialect: SQL Server (T-SQL)
-- ============================================================================
-- BUSINESS PROBLEM
-- Route planners and operations leaders needed one consistent measure of how
-- much of a route's available operating window was being used. The calculation
-- is not a simple division because:
--   * Future routes must use PLANNED elapsed time; completed routes use ACTUAL
--   * Source durations arrive in seconds
--   * Route windows can be missing, zero-length, or malformed
--   * A zero denominator must never break a dashboard or scheduled query
--   * Output must roll up to route, truck, domicile, and market level
--
-- NOTE: sanitized reconstruction of production logic — table and column names
-- are generalized; no proprietary data or configuration is included.
-- ============================================================================

WITH route_base AS (
    SELECT
        R.ROUTE_ID,
        R.MARKET_ID,
        R.TRUCK_ID,
        RE.EARLIEST_START_TS,
        RE.LATEST_END_TS,

        -- Planned vs actual is decided dynamically by route date:
        -- future routes have no actuals yet; past routes should never
        -- report the plan as if it happened.
        CASE
            WHEN CAST(RE.EARLIEST_START_TS AS DATE) >= CAST(GETDATE() AS DATE)
                THEN COALESCE(CAST(R.PLANNED_ELAPSED_SECONDS AS DECIMAL(18, 2)), 0)
            ELSE COALESCE(CAST(R.ACTUAL_ELAPSED_SECONDS AS DECIMAL(18, 2)), 0)
        END AS ELAPSED_SECONDS,

        -- Window in minutes (not hours) so short windows and routes that
        -- cross an hour boundary are measured safely.
        DATEDIFF(MINUTE, RE.EARLIEST_START_TS, RE.LATEST_END_TS) AS AVAILABLE_MINUTES
    FROM ROUTES AS R
    INNER JOIN ROUTE_EVENTS AS RE
        ON RE.ROUTE_ID = R.ROUTE_ID
),

route_metrics AS (
    SELECT
        ROUTE_ID,
        MARKET_ID,
        TRUCK_ID,
        ELAPSED_SECONDS / 60.0 AS ELAPSED_MINUTES,
        AVAILABLE_MINUTES,

        -- NULLIF guards the divide-by-zero; a NULL utilization is an honest
        -- "window invalid" signal rather than a fake 0% or a crashed query.
        (ELAPSED_SECONDS / 60.0)
            / NULLIF(CAST(AVAILABLE_MINUTES AS DECIMAL(18, 2)), 0) AS SAFE_HOURS_UTIL,

        AVAILABLE_MINUTES - (ELAPSED_SECONDS / 60.0) AS FREE_MINUTES
    FROM route_base
)

SELECT
    ROUTE_ID,
    MARKET_ID,
    TRUCK_ID,
    ROUND(ELAPSED_MINUTES / 60.0, 2)      AS RTE_ELAPSED_HOURS,
    AVAILABLE_MINUTES,
    ROUND(SAFE_HOURS_UTIL * 100.0, 1)     AS HOURS_UTILIZATION_PCT,
    ROUND(FREE_MINUTES, 0)                AS FREE_MINUTES,
    ROUND(
        FREE_MINUTES
        / NULLIF(CAST(AVAILABLE_MINUTES AS DECIMAL(18, 2)), 0) * 100.0,
        1
    )                                     AS FREE_CAPACITY_PCT
FROM route_metrics;

-- ============================================================================
-- WHY THIS IS A SYSTEMS PROBLEM, NOT JUST A FORMULA
-- A utilization metric can be numerically correct and still fail operationally
-- if planned and actual records are mixed, route windows are incomplete,
-- archived routes are treated like live routes, or the dashboard refreshes
-- before the final event arrives. The durable solution pairs this SQL with:
--   1. A clear source-of-truth rule for planned vs actual duration
--   2. Data-quality checks for invalid route windows
--   3. Explicit handling for archived vs live records
--   4. Refresh timing aligned to source-system availability
--   5. One metric definition shared by SQL and Tableau (never re-derived
--      independently in each workbook)
-- ============================================================================
