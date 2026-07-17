-- ============================================================================
-- Case Study 10: Incremental BigQuery→Tableau Load (Idempotent MERGE)
-- Dialect: BigQuery
-- ============================================================================
-- BUSINESS PROBLEM
-- Operational dashboards needed BOTH a small aggregated KPI layer and a
-- multi-million-row order-detail layer. One extract and one refresh schedule
-- for everything meant slow refreshes and unnecessary load. The architecture
-- separates them:
--
--   Operational source systems
--           |
--           v
--   BigQuery raw / staged data
--           |
--           +-- Curated order-detail fact
--           |     * partitioned by business date
--           |     * clustered by market / carrier / store
--           |     * incremental MERGE with a late-data lookback  <-- this file
--           |
--           +-- Pre-aggregated KPI fact
--                 * market/week/day summaries, small & fast to refresh
--           |
--           v
--   Tableau
--       * KPI source: broader but less frequent refresh
--       * Order-detail source: incremental, more frequent refresh
--
-- Different freshness and performance needs = different data products,
-- sharing conformed dimensions (market, carrier, fiscal week, store, vehicle).
--
-- NOTE: sanitized reconstruction — `portfolio_demo` replaces production names.
-- ============================================================================

-- Idempotent incremental load with a two-day lookback for late updates.
MERGE `portfolio_demo.curated_order_detail` AS target
USING (
    SELECT
        order_id,
        work_order_id,
        business_date,
        market_id,
        carrier_id,
        store_id,
        vehicle_id,
        delivery_status,
        invoiced_amount,
        updated_timestamp
    FROM `portfolio_demo.raw_order_events`
    -- Lookback window captures late-arriving or corrected source events
    WHERE updated_timestamp >= TIMESTAMP_SUB(
        @last_successful_refresh,
        INTERVAL 2 DAY
    )

    -- Latest-row qualification: repeated source events for the same order
    -- must not produce duplicate current records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id, work_order_id
        ORDER BY updated_timestamp DESC
    ) = 1
) AS source
ON  target.order_id      = source.order_id
AND target.work_order_id = source.work_order_id

-- Update only when the source is genuinely newer — reruns are harmless
WHEN MATCHED AND source.updated_timestamp > target.updated_timestamp
THEN UPDATE SET
    business_date     = source.business_date,
    market_id         = source.market_id,
    carrier_id        = source.carrier_id,
    store_id          = source.store_id,
    vehicle_id        = source.vehicle_id,
    delivery_status   = source.delivery_status,
    invoiced_amount   = source.invoiced_amount,
    updated_timestamp = source.updated_timestamp

WHEN NOT MATCHED THEN
INSERT (
    order_id, work_order_id, business_date, market_id, carrier_id,
    store_id, vehicle_id, delivery_status, invoiced_amount, updated_timestamp
)
VALUES (
    source.order_id, source.work_order_id, source.business_date,
    source.market_id, source.carrier_id, source.store_id, source.vehicle_id,
    source.delivery_status, source.invoiced_amount, source.updated_timestamp
);


-- ============================================================================
-- TABLE DESIGN: partition and cluster by the fields users actually filter on
-- ============================================================================
-- CREATE TABLE `portfolio_demo.curated_order_detail`
-- PARTITION BY business_date
-- CLUSTER BY market_id, carrier_id, store_id
-- AS SELECT ... ;

-- ============================================================================
-- DESIGN DECISIONS
--   * Idempotent merge: rerunning a load can never duplicate rows
--   * Two-day lookback absorbs late-arriving data without full reloads
--   * Tableau is NOT the transformation engine — expensive joins and
--     aggregation happen upstream in BigQuery
--   * KPI and detail layers carry independent refresh schedules and SLAs
--   * Market-volume-based phased rollout controlled both technical and
--     adoption risk when scaling the dashboards
-- ============================================================================
