-- ============================================================================
-- Case Study 4: Customer-Order Delivery Attempt & Missed-Reason Rollup
-- Dialect: BigQuery
-- ============================================================================
-- BUSINESS PROBLEM
-- A customer order can have multiple work orders and multiple delivery
-- attempts, so raw status-event detail produced repeated rows and inflated
-- counts. Reliability questions needed a ONE ROW PER CUSTOMER ORDER model:
--   * How many legitimate delivery attempts occurred (excluding cancellations
--     and non-attempt statuses)?
--   * Which work orders were involved? Which missed-delivery reasons occurred?
--   * What fiscal week and invoice value apply?
--   * Is this a single-, two-, or repeat-attempt order?
--
-- The core issue is GRAIN: the source is status-event level, the business
-- question is customer-order level.
--
-- NOTE: sanitized reconstruction — demo status codes; `portfolio_demo`
-- replaces production names.
-- ============================================================================

WITH order_rollup AS (
    SELECT
        customer_order_id,

        -- The source repeats a work order across status events, so the
        -- attempt count must be DISTINCT and conditional: cancellations and
        -- non-attempt statuses do not count as attempts.
        COUNT(DISTINCT CASE
            WHEN is_cancelled = 'N'
             AND delivery_status_code NOT IN (1, 6, 20)
            THEN work_order_id
        END) AS total_attempts,

        STRING_AGG(
            DISTINCT CAST(work_order_id AS STRING),
            ', '
            ORDER BY CAST(work_order_id AS STRING)
        ) AS work_order_list,

        -- Missed reasons: distinguish "no reason supplied" from
        -- "no missed event occurred"
        STRING_AGG(
            DISTINCT CASE
                WHEN delivery_status_code IN (10, 11, 19)
                THEN COALESCE(NULLIF(TRIM(missed_reason_code), ''), 'noneProvided')
            END,
            ', '
            ORDER BY CASE
                WHEN delivery_status_code IN (10, 11, 19)
                THEN COALESCE(NULLIF(TRIM(missed_reason_code), ''), 'noneProvided')
            END
        ) AS missed_reason_codes,

        MAX(fiscal_year_week) AS fiscal_year_week,

        -- Valid only because invoice amount repeats consistently across the
        -- order's rows — see the data-quality check at the bottom.
        MAX(invoiced_amount) AS invoiced_amount

    FROM `portfolio_demo.delivery_order_status`
    GROUP BY customer_order_id
),

classified AS (
    SELECT
        *,
        -- Bucketing happens AFTER the order-level collapse
        CASE
            WHEN total_attempts = 0 THEN 'No Valid Attempt'
            WHEN total_attempts = 1 THEN 'Single Attempt'
            WHEN total_attempts = 2 THEN 'Two Attempts'
            ELSE '3+ Attempts'
        END AS attempt_bucket
    FROM order_rollup
)

SELECT
    customer_order_id,
    fiscal_year_week,
    total_attempts,
    attempt_bucket,
    work_order_list,
    COALESCE(missed_reason_codes, 'No Missed Reason') AS missed_reason_codes,
    invoiced_amount
FROM classified;


-- ============================================================================
-- ALTERNATIVE: structured output for machine consumers
-- Downstream analytics should not parse comma-separated strings — use arrays.
-- ============================================================================
-- ARRAY_AGG(
--     DISTINCT IF(
--         delivery_status_code IN (10, 11, 19),
--         COALESCE(NULLIF(TRIM(missed_reason_code), ''), 'noneProvided'),
--         NULL
--     )
--     IGNORE NULLS
-- ) AS missed_reason_array


-- ============================================================================
-- DATA-QUALITY EXTENSION
-- MAX(invoiced_amount) assumes the order-level financial attribute is stable.
-- This check flags orders where that assumption is violated:
-- ============================================================================
SELECT
    customer_order_id,
    COUNT(DISTINCT invoiced_amount) AS distinct_invoice_amount_count
FROM `portfolio_demo.delivery_order_status`
GROUP BY customer_order_id
HAVING COUNT(DISTINCT invoiced_amount) > 1;
