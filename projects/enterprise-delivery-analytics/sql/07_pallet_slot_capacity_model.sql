-- ============================================================================
-- Case Study 7: Pallet Calculation & Slot-Capacity Segmentation
-- Dialect: ANSI-style SQL (portable across SQL Server / BigQuery)
-- ============================================================================
-- BUSINESS PROBLEM
-- In slot-based delivery markets, pallet demand must be translated into
-- available delivery capacity under a recurring operating model:
--   * One pallet consumes one slot
--   * Capacity may be reserved by customer segment (Pro/PPC vs DIY)
--   * Pro demand must not unintentionally block DIY demand, or vice versa
--   * Unused reserved capacity may need rebalancing
--   * The result feeds operational planning and downstream systems
--   * Demand must trace from order lines -> orders -> routes -> trucks -> markets
--
-- This logic fed downstream systems, so auditability mattered as much as the
-- numbers themselves.
--
-- NOTE: sanitized reconstruction — demo segment values; no production
-- capacity thresholds or market configurations are included.
-- ============================================================================

WITH order_pallets AS (
    SELECT
        O.ORDER_ID,
        O.DELIVERY_DATE,
        O.MARKET_ID,
        O.ROUTE_ID,
        O.TRUCK_ID,

        -- Customer segments are classified ONCE, here, from master data —
        -- never re-derived differently by each downstream report
        CASE
            WHEN UPPER(L.CUSTOMER_CLASSIFICATION) IN ('PRO', 'PPC') THEN 'PRO'
            ELSE 'DIY'
        END AS CUSTOMER_SEGMENT,

        O.SERVICE_TYPE,

        SUM(COALESCE(OL.PALLET_QUANTITY, 0)) AS ORDER_PALLETS,
        SUM(COALESCE(OL.LINE_WEIGHT, 0))     AS ORDER_WEIGHT,
        SUM(COALESCE(OL.LINE_SALES, 0))      AS ORDER_SALES
    FROM ORDER_HEADER AS O
    INNER JOIN ORDER_LINE AS OL
        ON OL.ORDER_ID = O.ORDER_ID
    LEFT JOIN LOCATION AS L
        ON L.LOCATION_ID = O.CUSTOMER_LOCATION_ID
    WHERE O.DELIVERY_DATE BETWEEN @START_DATE AND @END_DATE
    GROUP BY
        O.ORDER_ID, O.DELIVERY_DATE, O.MARKET_ID, O.ROUTE_ID, O.TRUCK_ID,
        CASE
            WHEN UPPER(L.CUSTOMER_CLASSIFICATION) IN ('PRO', 'PPC') THEN 'PRO'
            ELSE 'DIY'
        END,
        O.SERVICE_TYPE
),

market_demand AS (
    SELECT
        DELIVERY_DATE,
        MARKET_ID,
        SUM(CASE WHEN CUSTOMER_SEGMENT = 'DIY' THEN ORDER_PALLETS ELSE 0 END) AS DIY_DEMAND,
        SUM(CASE WHEN CUSTOMER_SEGMENT = 'PRO' THEN ORDER_PALLETS ELSE 0 END) AS PRO_DEMAND,
        SUM(ORDER_PALLETS)          AS TOTAL_PALLET_DEMAND,
        SUM(ORDER_WEIGHT)           AS TOTAL_WEIGHT,
        SUM(ORDER_SALES)            AS TOTAL_SALES,
        COUNT(DISTINCT ORDER_ID)    AS ORDER_COUNT,
        COUNT(DISTINCT TRUCK_ID)    AS TRUCK_COUNT
    FROM order_pallets
    GROUP BY DELIVERY_DATE, MARKET_ID
),

-- Capacity rules are EFFECTIVE-DATED configuration, not constants in code
capacity_rules AS (
    SELECT
        MARKET_ID,
        EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE,
        TOTAL_SLOT_CAPACITY,
        DIY_RESERVED_SLOTS,
        PRO_RESERVED_SLOTS,
        OVERFLOW_POLICY
    FROM MARKET_CAPACITY_CONFIGURATION
),

reserved_allocation AS (
    SELECT
        D.*,
        C.TOTAL_SLOT_CAPACITY,
        C.DIY_RESERVED_SLOTS,
        C.PRO_RESERVED_SLOTS,
        C.OVERFLOW_POLICY,

        CASE WHEN D.DIY_DEMAND < C.DIY_RESERVED_SLOTS
             THEN D.DIY_DEMAND ELSE C.DIY_RESERVED_SLOTS END AS DIY_RESERVED_USED,
        CASE WHEN D.PRO_DEMAND < C.PRO_RESERVED_SLOTS
             THEN D.PRO_DEMAND ELSE C.PRO_RESERVED_SLOTS END AS PRO_RESERVED_USED
    FROM market_demand AS D
    INNER JOIN capacity_rules AS C
        ON C.MARKET_ID = D.MARKET_ID
       AND D.DELIVERY_DATE BETWEEN C.EFFECTIVE_START_DATE AND C.EFFECTIVE_END_DATE
),

capacity_position AS (
    SELECT
        *,
        TOTAL_SLOT_CAPACITY - DIY_RESERVED_USED - PRO_RESERVED_USED AS UNALLOCATED_CAPACITY,

        CASE WHEN DIY_DEMAND > DIY_RESERVED_USED
             THEN DIY_DEMAND - DIY_RESERVED_USED ELSE 0 END AS DIY_UNMET_DEMAND,
        CASE WHEN PRO_DEMAND > PRO_RESERVED_USED
             THEN PRO_DEMAND - PRO_RESERVED_USED ELSE 0 END AS PRO_UNMET_DEMAND
    FROM reserved_allocation
)

SELECT
    DELIVERY_DATE,
    MARKET_ID,
    TOTAL_SLOT_CAPACITY,
    DIY_DEMAND,
    PRO_DEMAND,
    TOTAL_PALLET_DEMAND,
    DIY_RESERVED_USED,
    PRO_RESERVED_USED,
    UNALLOCATED_CAPACITY,
    DIY_UNMET_DEMAND,
    PRO_UNMET_DEMAND,

    ROUND(
        100.0 * TOTAL_PALLET_DEMAND
        / NULLIF(CAST(TOTAL_SLOT_CAPACITY AS DECIMAL(18, 2)), 0),
        1
    ) AS SLOT_UTILIZATION_PCT,

    -- Segment imbalance is a DIFFERENT problem than true overcapacity —
    -- the exception taxonomy keeps them distinguishable for operations
    CASE
        WHEN TOTAL_PALLET_DEMAND > TOTAL_SLOT_CAPACITY      THEN 'OVER CAPACITY'
        WHEN DIY_UNMET_DEMAND > 0 OR PRO_UNMET_DEMAND > 0   THEN 'SEGMENT IMBALANCE'
        WHEN UNALLOCATED_CAPACITY > 0                       THEN 'AVAILABLE CAPACITY'
        ELSE 'FULLY ALLOCATED'
    END AS CAPACITY_STATUS,

    ORDER_COUNT,
    TRUCK_COUNT,
    TOTAL_WEIGHT,
    TOTAL_SALES
FROM capacity_position;

-- ============================================================================
-- WHY THIS QUERY MATTERS
-- It makes the operating model AUDITABLE:
--   * Demand is traceable to order lines
--   * Customer segments are classified once
--   * Capacity rules are effective-dated configuration
--   * Reserved capacity and unmet demand are visible separately
--   * Downstream consumers receive a stable, explainable result
-- ============================================================================
