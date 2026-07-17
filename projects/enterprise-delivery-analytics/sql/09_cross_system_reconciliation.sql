-- ============================================================================
-- Case Study 9: Cross-System Order & Route Reconciliation
-- Dialect: BigQuery
-- ============================================================================
-- BUSINESS PROBLEM
-- Delivery status and route information arrive from multiple systems at
-- different times. Common data-quality failures:
--   * Duplicate event records and conflicting final statuses
--   * Orders present in one system but missing from another
--   * Missing timestamps and late-arriving completion events
--   * Dashboard totals that do not reconcile to operational systems
--
-- The fix is a reconciliation layer that dedupes each source to its latest
-- record, joins across systems with FULL OUTER JOINs so missing records
-- SURFACE instead of disappearing, and classifies every order into an
-- actionable exception taxonomy.
--
-- NOTE: sanitized reconstruction — `portfolio_demo` replaces production names.
-- ============================================================================

WITH order_latest AS (
    SELECT
        ORDER_ID, ORDER_STATUS, PROMISED_TS, COMPLETED_TS, UPDATED_TS
    FROM (
        SELECT
            O.*,
            ROW_NUMBER() OVER (
                PARTITION BY ORDER_ID ORDER BY UPDATED_TS DESC
            ) AS RECORD_RANK
        FROM `portfolio_demo.raw_order_events` AS O
    )
    WHERE RECORD_RANK = 1
),

route_latest AS (
    SELECT
        ORDER_ID, ROUTE_ID, ROUTE_STATUS, DRIVER_ID, UPDATED_TS
    FROM (
        SELECT
            R.*,
            ROW_NUMBER() OVER (
                PARTITION BY ORDER_ID ORDER BY UPDATED_TS DESC
            ) AS RECORD_RANK
        FROM `portfolio_demo.raw_route_events` AS R
    )
    WHERE RECORD_RANK = 1
),

execution_latest AS (
    SELECT
        ORDER_ID, EXECUTION_STATUS, EVENT_TS
    FROM (
        SELECT
            E.*,
            ROW_NUMBER() OVER (
                PARTITION BY ORDER_ID ORDER BY EVENT_TS DESC
            ) AS RECORD_RANK
        FROM `portfolio_demo.raw_delivery_execution_events` AS E
    )
    WHERE RECORD_RANK = 1
)

SELECT
    -- FULL OUTER JOINs mean the order id can come from any system
    COALESCE(O.ORDER_ID, R.ORDER_ID, E.ORDER_ID) AS ORDER_ID,

    O.ORDER_STATUS,
    R.ROUTE_ID,
    R.ROUTE_STATUS,
    E.EXECUTION_STATUS,
    O.PROMISED_TS,
    O.COMPLETED_TS,

    -- Exception taxonomy designed for ACTION, not just defect counting:
    -- each state maps to a different owner and remediation path
    CASE
        WHEN O.ORDER_ID IS NULL
            THEN 'MISSING FROM ORDER SYSTEM'
        WHEN R.ORDER_ID IS NULL
            THEN 'MISSING ROUTE'
        WHEN E.ORDER_ID IS NULL
            THEN 'MISSING EXECUTION EVENT'
        WHEN O.ORDER_STATUS = 'COMPLETED'
             AND E.EXECUTION_STATUS <> 'DELIVERED'
            THEN 'STATUS CONFLICT'
        WHEN O.COMPLETED_TS IS NULL
             AND E.EXECUTION_STATUS = 'DELIVERED'
            THEN 'COMPLETION TIMESTAMP MISSING'
        WHEN O.COMPLETED_TS > O.PROMISED_TS
            THEN 'LATE DELIVERY'
        ELSE 'RECONCILED'
    END AS RECONCILIATION_STATUS
FROM order_latest AS O
FULL OUTER JOIN route_latest AS R
    ON R.ORDER_ID = O.ORDER_ID
FULL OUTER JOIN execution_latest AS E
    ON E.ORDER_ID = COALESCE(O.ORDER_ID, R.ORDER_ID);

-- ============================================================================
-- OPERATIONAL CONTROL
-- A reconciliation table earns its keep when paired with:
--   * Daily counts by exception type, and AGING of unresolved exceptions
--   * Source-system ownership per exception class
--   * Drill-through from Tableau to the affected orders/routes
--   * An agreed process for closing or suppressing false positives
-- ============================================================================
