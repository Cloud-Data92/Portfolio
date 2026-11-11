-- ============================================================================
-- Case Study 12: Event Time vs. Ingestion Time
-- Dialect: BigQuery   (runnable: see ../runnable/README.md)
-- ============================================================================
-- BUSINESS PROBLEM
-- Every status event carries two timestamps:
--     event_ts     when the carrier says the thing happened
--     ingested_ts  when our platform received the webhook
-- They are usually seconds apart. When a carrier's webhooks queue up, or an
-- outage is repaired with a batch backfill, they can be hours apart -- and any
-- KPI with a time cutoff ("delivered by the promised time") gives a different
-- answer depending on which column it reads.
--
-- Three questions, in the order an analyst should ask them:
--   1. How late does each carrier's data normally arrive?
--   2. On which days was a feed unhealthy?
--   3. How far does the on-time KPI move if it is built on the wrong column?
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema. Not employer code; contains no production identifiers, data, or
-- configuration. See NOTICE.md.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Ingestion delay by carrier
-- A re-sent webhook is the same event received twice. Delay is measured to the
-- FIRST receipt, so retries do not inflate the average.
-- ----------------------------------------------------------------------------
WITH first_receipt AS (
    SELECT
        delivery_id,
        status,
        event_ts,
        MIN(ingested_ts) AS first_ingested_ts
    FROM `portfolio_demo.delivery_status_events`
    GROUP BY delivery_id, status, event_ts
),

delays AS (
    SELECT
        d.carrier_id,
        TIMESTAMP_DIFF(f.first_ingested_ts, f.event_ts, SECOND) / 60.0 AS delay_min
    FROM first_receipt AS f
    JOIN `portfolio_demo.deliveries` AS d
      ON d.delivery_id = f.delivery_id
)

SELECT
    c.carrier_name,
    COUNT(*)                                                    AS events,
    ROUND(AVG(x.delay_min), 2)                                  AS avg_delay_min,
    ROUND(APPROX_QUANTILES(x.delay_min, 100)[OFFSET(50)], 2)    AS p50_delay_min,
    ROUND(APPROX_QUANTILES(x.delay_min, 100)[OFFSET(95)], 2)    AS p95_delay_min,
    ROUND(100 * SAFE_DIVIDE(COUNTIF(x.delay_min > 15), COUNT(*)), 2)
                                                                AS pct_over_15_min
FROM delays AS x
JOIN `portfolio_demo.carriers` AS c
  ON c.carrier_id = x.carrier_id
GROUP BY c.carrier_name
ORDER BY avg_delay_min DESC;


-- ----------------------------------------------------------------------------
-- 2. Feed-health exceptions: carrier-days where the p95 delay exceeded an hour
-- The average in query 1 hides this; one bad afternoon is diluted by months of
-- normal traffic. Grouping by the day the event HAPPENED puts the problem on
-- the day operations felt it.
-- ----------------------------------------------------------------------------
WITH first_receipt AS (
    SELECT
        delivery_id,
        status,
        event_ts,
        MIN(ingested_ts) AS first_ingested_ts
    FROM `portfolio_demo.delivery_status_events`
    GROUP BY delivery_id, status, event_ts
)

SELECT
    c.carrier_name,
    DATE(f.event_ts)                                            AS event_date_utc,
    COUNT(*)                                                    AS events,
    ROUND(APPROX_QUANTILES(
        TIMESTAMP_DIFF(f.first_ingested_ts, f.event_ts, SECOND) / 60.0, 100
    )[OFFSET(95)], 1)                                           AS p95_delay_min,
    ROUND(MAX(TIMESTAMP_DIFF(f.first_ingested_ts, f.event_ts, SECOND)) / 3600.0, 1)
                                                                AS max_delay_hours
FROM first_receipt AS f
JOIN `portfolio_demo.deliveries` AS d ON d.delivery_id = f.delivery_id
JOIN `portfolio_demo.carriers`   AS c ON c.carrier_id  = d.carrier_id
GROUP BY c.carrier_name, event_date_utc
HAVING p95_delay_min > 60
ORDER BY event_date_utc, c.carrier_name;


-- ----------------------------------------------------------------------------
-- 3. What the wrong column does to the on-time KPI
-- On-time = first COMPLETED at or before promised_by_ts, over all deliveries
-- created in the trailing 28 complete local days. Computed twice.
-- ----------------------------------------------------------------------------
WITH params AS (
    SELECT DATE '2026-06-28' AS as_of_date   -- CURRENT_DATE() in production
),

completion AS (
    SELECT
        delivery_id,
        MIN(event_ts)    AS completed_event_ts,
        MIN(ingested_ts) AS completed_ingested_ts
    FROM `portfolio_demo.delivery_status_events`
    WHERE status = 'COMPLETED'
    GROUP BY delivery_id
)

SELECT
    c.carrier_name,
    COUNT(*)                                                    AS deliveries,
    ROUND(100 * SAFE_DIVIDE(
        COUNTIF(k.completed_event_ts <= d.promised_by_ts), COUNT(*)), 2)
                                                                AS on_time_pct_event_time,
    ROUND(100 * SAFE_DIVIDE(
        COUNTIF(k.completed_ingested_ts <= d.promised_by_ts), COUNT(*)), 2)
                                                                AS on_time_pct_ingest_time,
    COUNTIF(k.completed_event_ts <= d.promised_by_ts
            AND k.completed_ingested_ts > d.promised_by_ts)     AS on_time_but_reported_late
FROM `portfolio_demo.deliveries` AS d
JOIN `portfolio_demo.stores`   AS s ON s.store_id   = d.store_id
JOIN `portfolio_demo.carriers` AS c ON c.carrier_id = d.carrier_id
CROSS JOIN params AS p
LEFT JOIN completion AS k ON k.delivery_id = d.delivery_id
WHERE DATE(d.created_ts, s.time_zone)
      BETWEEN DATE_SUB(p.as_of_date, INTERVAL 28 DAY)
          AND DATE_SUB(p.as_of_date, INTERVAL 1 DAY)
GROUP BY c.carrier_name
ORDER BY on_time_but_reported_late DESC;


-- ============================================================================
-- DESIGN DECISIONS
--  * Operational KPIs are built on event_ts. ingested_ts answers a different
--    question -- "how fresh and how trustworthy is this feed?" -- and is kept
--    for exactly that.
--  * Percentiles, not just averages: latency is long-tailed, and the tail is
--    where the decisions get distorted.
--  * Investigation order when a time-cutoff KPI moves unexpectedly: check feed
--    freshness first (query 2), then compare event vs. ingestion time
--    (query 3), then corroborate against the carrier's own portal before
--    anyone is told their performance dropped.
--  * ingested_ts still matters for incremental loads: the late-data lookback
--    in case study 10 is sized from the p95/max figures this file produces.
-- ============================================================================
