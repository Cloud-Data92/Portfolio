-- ============================================================================
-- Case Study 13: Pickup Dwell Time Across Rolling Windows (LW / R4 / R13)
-- Dialect: BigQuery   (runnable: see ../runnable/README.md)
-- ============================================================================
-- BUSINESS PROBLEM
-- Dwell time is how long a driver waits at the store between arriving for a
-- pickup and leaving with the order. Long dwell costs driver pay, pushes
-- deliveries past their promise time and is almost entirely within the
-- store's control -- so store leaders ask for it constantly.
--
-- There is no "dwell" column. It has to be built from two status events
-- (ARRIVED_PICKUP, DEPARTED_PICKUP) that can be duplicated, missing or
-- implausible, and then compared across the windows operations actually
-- uses: last week (LW), rolling 4 weeks (R4) and rolling 13 weeks (R13).
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema. Not employer code; contains no production identifiers, data, or
-- configuration. See NOTICE.md.
-- ============================================================================

WITH params AS (
    SELECT DATE '2026-06-28' AS as_of_date   -- CURRENT_DATE() in production
),

week_bounds AS (
    -- Weeks run Sunday-Saturday. "Last week" is the most recent COMPLETE week.
    SELECT
        DATE_SUB(DATE_TRUNC(as_of_date, WEEK(SUNDAY)), INTERVAL 7  DAY) AS lw_start,
        DATE_SUB(DATE_TRUNC(as_of_date, WEEK(SUNDAY)), INTERVAL 28 DAY) AS r4_start,
        DATE_SUB(DATE_TRUNC(as_of_date, WEEK(SUNDAY)), INTERVAL 91 DAY) AS r13_start,
        DATE_TRUNC(as_of_date, WEEK(SUNDAY))                            AS current_week_start
    FROM params
),

-- One row per delivery that reached the store. MIN() collapses duplicate
-- webhooks; the departure must come after the arrival to count as a pair.
pickup_pairs AS (
    SELECT
        e.delivery_id,
        MIN(IF(e.status = 'ARRIVED_PICKUP',  e.event_ts, NULL)) AS arrived_ts,
        MIN(IF(e.status = 'DEPARTED_PICKUP', e.event_ts, NULL)) AS departed_ts
    FROM `portfolio_demo.delivery_status_events` AS e
    WHERE e.status IN ('ARRIVED_PICKUP', 'DEPARTED_PICKUP')
    GROUP BY e.delivery_id
    HAVING arrived_ts IS NOT NULL
),

dwell AS (
    SELECT
        d.store_id,
        s.market,
        -- Bucket by the store's local date of arrival, not the UTC date
        DATE(p.arrived_ts, s.time_zone)                         AS local_arrival_date,
        TIMESTAMP_DIFF(p.departed_ts, p.arrived_ts, SECOND) / 60.0 AS dwell_min,
        -- A dwell is usable only when both events exist, are in order, and
        -- the gap is physically plausible. Everything else is counted, not
        -- averaged.
        (p.departed_ts IS NOT NULL
         AND p.departed_ts >= p.arrived_ts
         AND TIMESTAMP_DIFF(p.departed_ts, p.arrived_ts, MINUTE) <= 180) AS is_usable
    FROM pickup_pairs AS p
    JOIN `portfolio_demo.deliveries` AS d ON d.delivery_id = p.delivery_id
    JOIN `portfolio_demo.stores`     AS s ON s.store_id    = d.store_id
)

SELECT
    x.store_id,
    x.market,
    COUNTIF(x.local_arrival_date >= w.lw_start)                 AS lw_pickups,
    ROUND(AVG(IF(x.is_usable AND x.local_arrival_date >= w.lw_start,
                 x.dwell_min, NULL)), 1)                        AS lw_avg_dwell_min,
    ROUND(AVG(IF(x.is_usable AND x.local_arrival_date >= w.r4_start,
                 x.dwell_min, NULL)), 1)                        AS r4_avg_dwell_min,
    ROUND(AVG(IF(x.is_usable, x.dwell_min, NULL)), 1)           AS r13_avg_dwell_min,
    ROUND(AVG(IF(x.is_usable AND x.local_arrival_date >= w.lw_start,
                 x.dwell_min, NULL))
          - AVG(IF(x.is_usable, x.dwell_min, NULL)), 1)         AS lw_vs_r13_min,
    -- Coverage: how much of the R13 window could not be measured at all
    ROUND(100 * SAFE_DIVIDE(COUNTIF(NOT x.is_usable), COUNT(*)), 2)
                                                                AS r13_unmeasured_pct
FROM dwell AS x
CROSS JOIN week_bounds AS w
-- One scan of the widest window; the narrower windows are conditional
-- aggregates over the same rows instead of three separate queries.
WHERE x.local_arrival_date >= w.r13_start
  AND x.local_arrival_date <  w.current_week_start
GROUP BY x.store_id, x.market
ORDER BY r13_avg_dwell_min DESC, x.store_id;


-- ============================================================================
-- Dwell by local hour of arrival (network-wide, R13)
-- The hour is the STORE's wall-clock hour. Grouping on the UTC hour would
-- smear four time zones together and hide the midday peak.
-- ============================================================================
WITH params AS (
    SELECT DATE '2026-06-28' AS as_of_date
),

pickup_pairs AS (
    SELECT
        e.delivery_id,
        MIN(IF(e.status = 'ARRIVED_PICKUP',  e.event_ts, NULL)) AS arrived_ts,
        MIN(IF(e.status = 'DEPARTED_PICKUP', e.event_ts, NULL)) AS departed_ts
    FROM `portfolio_demo.delivery_status_events` AS e
    WHERE e.status IN ('ARRIVED_PICKUP', 'DEPARTED_PICKUP')
    GROUP BY e.delivery_id
    HAVING arrived_ts IS NOT NULL AND departed_ts >= arrived_ts
)

SELECT
    EXTRACT(HOUR FROM p.arrived_ts AT TIME ZONE s.time_zone)    AS local_arrival_hour,
    COUNT(*)                                                    AS pickups,
    ROUND(AVG(TIMESTAMP_DIFF(p.departed_ts, p.arrived_ts, SECOND) / 60.0), 1)
                                                                AS avg_dwell_min,
    ROUND(APPROX_QUANTILES(
        TIMESTAMP_DIFF(p.departed_ts, p.arrived_ts, SECOND) / 60.0, 100
    )[OFFSET(90)], 1)                                           AS p90_dwell_min
FROM pickup_pairs AS p
JOIN `portfolio_demo.deliveries` AS d ON d.delivery_id = p.delivery_id
JOIN `portfolio_demo.stores`     AS s ON s.store_id    = d.store_id
CROSS JOIN params AS q
WHERE DATE(p.arrived_ts, s.time_zone)
      >= DATE_SUB(DATE_TRUNC(q.as_of_date, WEEK(SUNDAY)), INTERVAL 91 DAY)
  AND DATE(p.arrived_ts, s.time_zone) < DATE_TRUNC(q.as_of_date, WEEK(SUNDAY))
  AND TIMESTAMP_DIFF(p.departed_ts, p.arrived_ts, MINUTE) <= 180
GROUP BY local_arrival_hour
ORDER BY local_arrival_hour;


-- ============================================================================
-- DESIGN DECISIONS
--  * Unmeasurable pickups are reported as a coverage percentage beside the
--    average. An average over 60% of pickups is a different number from an
--    average over 99% of them, and the reader should be able to tell which.
--  * LW, R4 and R13 come from one pass with conditional aggregation, so the
--    three windows can never disagree about which rows they looked at.
--  * lw_vs_r13_min compares a store to its own baseline. A store that is
--    always slow and a store that got slow last week need different
--    conversations.
--  * The 180-minute plausibility cap is a parameter of this demo, stated
--    here rather than buried in a WHERE clause.
-- ============================================================================
