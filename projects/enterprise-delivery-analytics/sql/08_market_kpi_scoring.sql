-- ============================================================================
-- Case Study 8: Weighted Market Opportunity & KPI Heatmap Scoring
-- Dialect: BigQuery
-- ============================================================================
-- BUSINESS PROBLEM
-- Markets had to be compared across metrics with different scales and meanings.
-- The scoring design grouped measures into four dimensions — Volume,
-- Operations, Capacity, Quality — each a weighted component of an overall
-- priority score. The hard part was not adding metrics: it was making unlike
-- metrics comparable, preserving whether HIGH or LOW values are desirable,
-- and keeping the score explainable to leadership.
--
-- The scored output fed the market KPI heatmap used for routing and capacity
-- decisions (see assets/kpi_heatmap.png for the visual layout).
--
-- NOTE: sanitized reconstruction — the Capacity and Quality weights shown are
-- illustrative public values, not production configuration.
-- ============================================================================

WITH weights AS (
    SELECT
        0.30 AS volume_weight,
        0.30 AS operations_weight,
        0.20 AS capacity_weight,      -- illustrative
        0.20 AS quality_weight        -- illustrative
),

metric_deltas AS (
    SELECT
        market_id,
        market_name,

        SAFE_DIVIDE(
            deliveries - delivery_forecast,
            NULLIF(delivery_forecast, 0)
        ) AS delivery_vs_forecast_pct,

        SAFE_DIVIDE(
            units_ty - units_ly,
            NULLIF(units_ly, 0)
        ) AS unit_growth_pct,

        SAFE_DIVIDE(
            actual_trucks_loaded - planned_trucks_loaded,
            NULLIF(planned_trucks_loaded, 0)
        ) AS trucks_vs_plan_pct,

        SAFE_DIVIDE(
            actual_route_hours - planned_route_hours,
            NULLIF(planned_route_hours, 0)
        ) AS route_hours_over_plan_pct,

        SAFE_DIVIDE(
            hours_utilization_ty - hours_utilization_ly,
            NULLIF(hours_utilization_ly, 0)
        ) AS utilization_change_pct,

        slot_utilization_pct,
        unserved_capacity_pct,
        missed_delivery_rate,
        defect_or_claim_rate

    FROM `portfolio_demo.market_weekly_metrics`
),

normalized AS (
    SELECT
        *,

        -- PERCENT_RANK puts every metric on a common 0-1 scale that is
        -- robust to outliers and easy to explain across markets.

        -- Higher over-forecast / growth values = volume pressure
        PERCENT_RANK() OVER (ORDER BY delivery_vs_forecast_pct) AS norm_delivery_pressure,
        PERCENT_RANK() OVER (ORDER BY unit_growth_pct)          AS norm_unit_growth,

        -- Higher over-plan values = operational risk
        PERCENT_RANK() OVER (ORDER BY trucks_vs_plan_pct)         AS norm_truck_variance,
        PERCENT_RANK() OVER (ORDER BY route_hours_over_plan_pct)  AS norm_route_hour_variance,

        -- METRIC DIRECTION IS EXPLICIT: a utilization DECLINE is inverted
        -- so every normalized metric reads "higher = needs more attention"
        PERCENT_RANK() OVER (ORDER BY -utilization_change_pct)    AS norm_utilization_decline,

        PERCENT_RANK() OVER (ORDER BY slot_utilization_pct)       AS norm_slot_pressure,
        PERCENT_RANK() OVER (ORDER BY unserved_capacity_pct)      AS norm_unserved_capacity,
        PERCENT_RANK() OVER (ORDER BY missed_delivery_rate)       AS norm_missed_delivery,
        PERCENT_RANK() OVER (ORDER BY defect_or_claim_rate)       AS norm_quality_issue

    FROM metric_deltas
),

component_scores AS (
    SELECT
        *,
        (norm_delivery_pressure + norm_unit_growth) / 2.0 AS volume_score,

        (norm_truck_variance
         + norm_route_hour_variance
         + norm_utilization_decline) / 3.0                AS operations_score,

        (norm_slot_pressure + norm_unserved_capacity) / 2.0 AS capacity_score,

        (norm_missed_delivery + norm_quality_issue) / 2.0   AS quality_score
    FROM normalized
)

SELECT
    market_id,
    market_name,
    -- Component scores stay VISIBLE so leaders can see WHY a market ranks
    -- highly — a single opaque number would not survive its first meeting
    ROUND(100 * volume_score, 1)     AS volume_score,
    ROUND(100 * operations_score, 1) AS operations_score,
    ROUND(100 * capacity_score, 1)   AS capacity_score,
    ROUND(100 * quality_score, 1)    AS quality_score,

    ROUND(
        100 * (
            volume_score     * volume_weight
          + operations_score * operations_weight
          + capacity_score   * capacity_weight
          + quality_score    * quality_weight
        ),
        1
    ) AS overall_priority_score

FROM component_scores
CROSS JOIN weights
ORDER BY overall_priority_score DESC;

-- ============================================================================
-- DESIGN DECISIONS
--   * Weights are centralized in one CTE, not scattered through the query
--   * Null and zero-denominator behavior is defined, never silently distorted
--   * A score PRIORITIZES INVESTIGATION — it does not replace judgment
--   * The Tableau layer visualizes these already-defined metrics; core
--     business logic is never re-implemented per workbook
-- ============================================================================
