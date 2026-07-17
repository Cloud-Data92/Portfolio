-- ============================================================================
-- Validation & Data-Quality Queries
-- ============================================================================
-- A metric layer is only as trustworthy as its checks. These queries ran (in
-- sanitized form here) alongside the case-study logic to prove correctness —
-- a portfolio should show how results were VALIDATED, not only produced.
-- ============================================================================

-- 1. Grain uniqueness: the one-row-per-order model must actually be one row
--    per order. Expected result: zero rows.
SELECT
    customer_order_id,
    COUNT(*) AS row_count
FROM `portfolio_demo.customer_order_rollup`
GROUP BY customer_order_id
HAVING COUNT(*) > 1;


-- 2. Conflicting "stable" attributes: invoice amount should repeat
--    consistently across an order's status rows. Rows here mean the
--    MAX(invoiced_amount) assumption in the rollup is unsafe for that order.
SELECT
    customer_order_id,
    COUNT(DISTINCT invoiced_amount) AS invoice_value_count
FROM `portfolio_demo.delivery_order_status`
GROUP BY customer_order_id
HAVING COUNT(DISTINCT invoiced_amount) > 1;


-- 3. Missing or invalid geospatial data: bad coordinates silently corrupt
--    every distance calculation downstream.
SELECT
    store_id,
    latitude,
    longitude
FROM `portfolio_demo.store_dimension`
WHERE latitude IS NULL
   OR longitude IS NULL
   OR latitude  NOT BETWEEN -90  AND 90
   OR longitude NOT BETWEEN -180 AND 180;


-- 4. Coverage gaps: ZIPs with no eligible store inside any service radius.
SELECT z.zip_code
FROM `portfolio_demo.zip_dimension` AS z
LEFT JOIN `portfolio_demo.zip_store_assignment` AS a
  ON a.zip_code = z.zip_code
WHERE a.zip_code IS NULL;


-- 5. Utilization outliers: values above 100% can be legitimate over-capacity
--    signals, but extremes indicate a data problem, not an operational one.
SELECT *
FROM `portfolio_demo.delivery_slot_utilization`
WHERE slot_utilization < 0
   OR slot_utilization > 2.0;


-- 6. Current-state uniqueness: the latest-alert view must return exactly one
--    row per stop and route. Expected result: zero rows.
SELECT
    stop_id,
    route_id,
    COUNT(*) AS row_count
FROM `portfolio_demo.current_route_alert`
GROUP BY stop_id, route_id
HAVING COUNT(*) > 1;
