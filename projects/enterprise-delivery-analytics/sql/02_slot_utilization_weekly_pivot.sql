-- ============================================================================
-- Case Study 2: Current-Week Delivery Slot Utilization Pivot
-- Dialect: BigQuery
-- ============================================================================
-- BUSINESS PROBLEM
-- Field and market leaders needed a current-week view of how much delivery-slot
-- capacity had been consumed. Source data was long-form (one or more records
-- per date x market x delivery type x ZIP group) — hard to scan operationally.
-- Required output:
--   * One row per delivery market x delivery type x ZIP group
--   * Monday through Saturday as columns
--   * Utilization = pallets sold / slot capacity, safe against zero capacity
--   * Dynamic current-week filter (never hard-coded dates)
--
-- NOTE: sanitized reconstruction — `portfolio_demo` replaces production
-- project/dataset names.
-- ============================================================================

WITH parameters AS (
    SELECT
        DATE_TRUNC(CURRENT_DATE('America/New_York'), WEEK(MONDAY)) AS week_start
),

pre_pivot AS (
    SELECT
        delivery_market,
        delivery_type,
        zip_group,
        FORMAT_DATE('%A', calendar_date) AS day_of_week,

        -- Aggregate numerator and denominator BEFORE dividing: averaging
        -- row-level ratios is mathematically wrong when multiple records
        -- exist per day at the reporting grain.
        SAFE_DIVIDE(
            SUM(pallets_sold),
            NULLIF(SUM(slot_capacity), 0)
        ) AS slot_utilization

    FROM `portfolio_demo.delivery_slot_daily`
    CROSS JOIN parameters
    WHERE current_year_indicator = 'TY'
      AND calendar_date BETWEEN week_start
                            AND DATE_ADD(week_start, INTERVAL 5 DAY)
    GROUP BY
        delivery_market,
        delivery_type,
        zip_group,
        day_of_week
),

pivoted AS (
    SELECT *
    FROM pre_pivot
    PIVOT (
        MAX(slot_utilization)
        FOR day_of_week IN (
            'Monday'    AS monday,
            'Tuesday'   AS tuesday,
            'Wednesday' AS wednesday,
            'Thursday'  AS thursday,
            'Friday'    AS friday,
            'Saturday'  AS saturday
        )
    )
)

SELECT
    delivery_market,
    delivery_type,
    zip_group,
    ROUND(100 * monday,    2) AS monday_utilization_pct,
    ROUND(100 * tuesday,   2) AS tuesday_utilization_pct,
    ROUND(100 * wednesday, 2) AS wednesday_utilization_pct,
    ROUND(100 * thursday,  2) AS thursday_utilization_pct,
    ROUND(100 * friday,    2) AS friday_utilization_pct,
    ROUND(100 * saturday,  2) AS saturday_utilization_pct
FROM pivoted
ORDER BY
    delivery_market,
    delivery_type,
    zip_group;

-- ============================================================================
-- DESIGN DECISIONS
--   * SAFE_DIVIDE + NULLIF: zero capacity yields NULL, not a crash or Infinity
--   * Dynamic week boundary: the report always follows the current
--     Monday-Saturday operating week without manual edits
--   * Percentages stay numeric so Tableau can format and aggregate them —
--     concatenating '%' would silently convert the metric to text
--   * Grouping keys fixed BEFORE the pivot prevents accidental row explosion
-- ============================================================================
