-- ============================================================================
-- 03: Automated KPI Alerts (seasonality-aware anomaly detection in SQL)
-- Dialect: BigQuery   (runnable: python3 run.py)
-- ============================================================================
-- BUSINESS PROBLEM
-- Operations tracks daily KPIs per org. Nobody can watch 18 series by eye, so
-- the ask is an alert feed: tell me when a metric moves more than normal
-- variation explains, and tell me when a day of data is missing.
--
-- The obvious method -- compare today with the trailing 28-day average -- fails
-- on any metric with a weekly rhythm: ticket volume is always lower at the
-- weekend, so the rhythm itself inflates the "normal" spread and real
-- problems hide inside it. This version removes the weekly pattern first.
--
--   1. Baseline  = average of the same weekday over the prior 8 weeks
--   2. Residual  = today minus that baseline
--   3. Spread    = standard deviation of residuals over the prior 56 days
--   4. z-score   = residual / spread; alert at |z| >= 4
--
-- The current day is never part of its own baseline (frames end at
-- 1 PRECEDING), otherwise a spike would partly excuse itself.
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema and synthetic data. Not employer code. See NOTICE.md.
-- ============================================================================

WITH weekday_baseline AS (
    SELECT
        org_id,
        metric_name,
        metric_date,
        metric_value,
        AVG(metric_value) OVER same_weekday   AS baseline_value,
        COUNT(*)          OVER same_weekday   AS baseline_points
    FROM `stratops_demo.kpi_daily`
    WINDOW same_weekday AS (
        PARTITION BY org_id, metric_name, EXTRACT(DAYOFWEEK FROM metric_date)
        ORDER BY metric_date
        ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING
    )
),

residuals AS (
    SELECT
        *,
        metric_value - baseline_value AS residual,
        STDDEV_SAMP(metric_value - baseline_value) OVER (
            PARTITION BY org_id, metric_name
            ORDER BY metric_date
            ROWS BETWEEN 56 PRECEDING AND 1 PRECEDING
        ) AS residual_spread
    FROM weekday_baseline
    WHERE baseline_points = 8                 -- only score days with a full baseline
)

SELECT
    r.metric_date,
    o.org_name,
    r.metric_name,
    r.metric_value,
    ROUND(r.baseline_value, 2)                               AS expected_value,
    ROUND(100 * SAFE_DIVIDE(r.residual, r.baseline_value), 1) AS pct_vs_expected,
    ROUND(SAFE_DIVIDE(r.residual, r.residual_spread), 1)     AS z_score
FROM residuals AS r
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = r.org_id
WHERE r.metric_date >= DATE '2026-08-01'      -- alert window; earlier rows are warm-up
  AND ABS(SAFE_DIVIDE(r.residual, r.residual_spread)) >= 4
ORDER BY r.metric_date, o.org_name, r.metric_name;

-- ----------------------------------------------------------------------------
-- Missing-data alerts: a gap in a daily series is its own kind of anomaly,
-- and one that a z-score can never see because there is no row to score.
-- ----------------------------------------------------------------------------
WITH gaps AS (
    SELECT
        org_id,
        metric_name,
        metric_date,
        LAG(metric_date) OVER (PARTITION BY org_id, metric_name ORDER BY metric_date)
            AS previous_date
    FROM `stratops_demo.kpi_daily`
)

SELECT
    o.org_name,
    g.metric_name,
    DATE_ADD(g.previous_date, INTERVAL 1 DAY)                AS first_missing_date,
    DATE_DIFF(g.metric_date, g.previous_date, DAY) - 1       AS missing_days
FROM gaps AS g
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = g.org_id
WHERE DATE_DIFF(g.metric_date, g.previous_date, DAY) > 1
ORDER BY first_missing_date, o.org_name;

-- ============================================================================
-- DESIGN DECISIONS
--  * The threshold is 4, not the textbook 3. With 18 series scored daily, a
--    3-sigma rule produces a false alert every few weeks, and an alert feed
--    people learn to ignore is worse than none.
--  * Known limit: a real anomaly enters later baselines and briefly widens the
--    spread, making the following weeks slightly less sensitive. A production
--    version would exclude flagged days from future baselines.
--  * Alerts report percent-versus-expected beside the z-score. The z-score
--    says "this is unusual"; the percentage says whether anyone should care.
-- ============================================================================
