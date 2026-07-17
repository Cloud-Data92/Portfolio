-- ============================================================================
-- Case Study 5: Latest Actionable Alert per Stop and Route
-- Dialect: BigQuery
-- ============================================================================
-- BUSINESS PROBLEM
-- A stop or route accumulates many alerts over time, but a dashboard or
-- operational feed needs the CURRENT alert state — not the full history.
-- MAX(alert_datetime) alone is insufficient: the business also needs the alert
-- ID, code, and decoded description FROM THE SAME latest row. This is a
-- temporal-state problem: preserve the whole latest record, break timestamp
-- ties deterministically, then enrich from a code dimension.
--
-- NOTE: sanitized reconstruction — `portfolio_demo` replaces production names.
-- ============================================================================

WITH ranked_alerts AS (
    SELECT
        stop_id,
        route_id,
        alert_id,
        alert_code_id,
        alert_datetime,

        ROW_NUMBER() OVER (
            PARTITION BY stop_id, route_id
            ORDER BY
                alert_datetime DESC,
                alert_id DESC          -- two alerts can share a timestamp;
                                       -- alert_id makes ordering stable
        ) AS recency_rank

    FROM `portfolio_demo.route_alert_history`
    WHERE is_deleted = FALSE
)

SELECT
    a.stop_id,
    a.route_id,
    a.alert_id,
    a.alert_datetime,
    a.alert_code_id,
    c.alert_code_key,
    c.alert_description
FROM ranked_alerts AS a
-- LEFT JOIN keeps the alert visible even when a new or malformed code is
-- missing from the dimension — data-quality gaps surface instead of hiding.
LEFT JOIN `portfolio_demo.alert_code_dimension` AS c
  ON c.alert_code_id = a.alert_code_id
WHERE a.recency_rank = 1;


-- ============================================================================
-- EQUIVALENT USING QUALIFY (BigQuery-idiomatic, no nested CTE needed)
-- ============================================================================
SELECT
    a.stop_id,
    a.route_id,
    a.alert_id,
    a.alert_datetime,
    a.alert_code_id,
    c.alert_code_key,
    c.alert_description
FROM `portfolio_demo.route_alert_history` AS a
LEFT JOIN `portfolio_demo.alert_code_dimension` AS c
  ON c.alert_code_id = a.alert_code_id
WHERE a.is_deleted = FALSE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.stop_id, a.route_id
    ORDER BY a.alert_datetime DESC, a.alert_id DESC
) = 1;

-- ============================================================================
-- DESIGN DECISIONS
--   * Latest-record logic is a MODELING decision, not a shortcut: the raw
--     history table stays intact for audit, while the ranked current-state
--     layer gives operations exactly one actionable alert per stop and route
--   * ROW_NUMBER preserves row integrity — every output column comes from the
--     same physical latest record (unlike mixing MAX() aggregates)
--   * Deterministic tie-breaking prevents dashboards from flickering between
--     equally-timestamped alerts on refresh
-- ============================================================================
