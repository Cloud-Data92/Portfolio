-- ============================================================================
-- Case Study 11: Terminal Outcome Precedence & Trailing-28-Day Missed Rate
-- Dialect: BigQuery   (runnable: see ../runnable/README.md)
-- ============================================================================
-- BUSINESS PROBLEM
-- A carrier sends a webhook for every status change, so one delivery has many
-- event rows -- sometimes duplicated, sometimes out of order, sometimes
-- contradictory (MISSED at 2pm, COMPLETED on a redelivery the next morning).
-- The store scorecard needs ONE outcome per delivery and a missed-delivery
-- rate per store over the trailing 28 complete days.
--
-- "Take the latest row" is the tempting answer and it is wrong. The outcome is
-- decided by a fixed status priority, not by recency. This demo uses the
-- simplest common form of that pattern -- a priority number per status, lowest
-- number wins:
--
--   1  COMPLETED   a redelivery that succeeded is a success
--   2  MISSED      a failed attempt outranks a later cancellation
--   3  CANCELED
--   -  (none)      no terminal status at all is PENDING
--
-- The priority order is illustrative. The point is the technique: the order
-- lives in one small lookup, so changing the rule is a one-line edit.
--
-- Canceled and pending deliveries stay in the denominator: dropping them
-- flatters every store that cancels its problem orders.
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema and an illustrative, generic priority order. Not employer code; contains no
-- production identifiers, data, or configuration. See NOTICE.md.
-- ============================================================================

WITH params AS (
    -- Pinned so committed outputs reproduce. In production: CURRENT_DATE().
    SELECT DATE '2026-06-28' AS as_of_date
),

window_deliveries AS (
    SELECT
        d.delivery_id,
        d.store_id,
        s.market
    FROM `portfolio_demo.deliveries` AS d
    JOIN `portfolio_demo.stores` AS s
      ON s.store_id = d.store_id
    CROSS JOIN params AS p
    -- The business day is the STORE's local day, not the UTC day. A 9pm
    -- Pacific order is already "tomorrow" in UTC.
    WHERE DATE(d.created_ts, s.time_zone)
          BETWEEN DATE_SUB(p.as_of_date, INTERVAL 28 DAY)
              AND DATE_SUB(p.as_of_date, INTERVAL 1 DAY)
),

-- One row per delivery, built by aggregating to the best priority rather than
-- ranking rows to find a "latest" one. MIN() is indifferent to duplicate
-- webhooks and to arrival order, so no dedup step is needed and none can be
-- forgotten. Neither timestamp plays any part in deciding the outcome.
status_priority AS (
    SELECT 'COMPLETED' AS status, 1 AS priority UNION ALL
    SELECT 'MISSED',              2             UNION ALL
    SELECT 'CANCELED',            3
),

terminal_facts AS (
    SELECT
        e.delivery_id,
        MIN(sp.priority) AS best_priority
    FROM `portfolio_demo.delivery_status_events` AS e
    JOIN status_priority AS sp
      ON sp.status = e.status          -- non-terminal statuses drop out here
    GROUP BY e.delivery_id
),

outcomes AS (
    SELECT
        w.delivery_id,
        w.store_id,
        w.market,
        COALESCE(sp.status, 'PENDING') AS outcome
    FROM window_deliveries AS w
    -- LEFT JOIN: a delivery with no terminal event must survive as PENDING.
    -- Any filter on t.* belongs in the ON clause or the CASE, never in WHERE,
    -- or this silently becomes an inner join and the denominator shrinks.
    LEFT JOIN terminal_facts AS t
      ON t.delivery_id = w.delivery_id
    LEFT JOIN status_priority AS sp
      ON sp.priority = t.best_priority
)

SELECT
    store_id,
    market,
    COUNT(*)                                              AS deliveries,
    COUNTIF(outcome = 'COMPLETED')                        AS completed,
    COUNTIF(outcome = 'MISSED')                           AS missed,
    COUNTIF(outcome = 'CANCELED')                         AS canceled,
    COUNTIF(outcome = 'PENDING')                          AS pending,
    ROUND(100 * SAFE_DIVIDE(COUNTIF(outcome = 'MISSED'), COUNT(*)), 2)
                                                          AS missed_rate_pct
FROM outcomes
GROUP BY store_id, market
ORDER BY missed_rate_pct DESC, store_id;


-- ============================================================================
-- WHAT THE SHORTCUT COSTS
-- The same deliveries classified two ways: the precedence rule above versus
-- "status of the most recently INGESTED terminal row". Every off-diagonal cell
-- is a delivery the shortcut reports wrongly.
-- ============================================================================
WITH params AS (
    SELECT DATE '2026-06-28' AS as_of_date
),

window_deliveries AS (
    SELECT d.delivery_id
    FROM `portfolio_demo.deliveries` AS d
    JOIN `portfolio_demo.stores` AS s
      ON s.store_id = d.store_id
    CROSS JOIN params AS p
    WHERE DATE(d.created_ts, s.time_zone)
          BETWEEN DATE_SUB(p.as_of_date, INTERVAL 28 DAY)
              AND DATE_SUB(p.as_of_date, INTERVAL 1 DAY)
),

status_priority AS (
    SELECT 'COMPLETED' AS status, 1 AS priority UNION ALL
    SELECT 'MISSED',              2             UNION ALL
    SELECT 'CANCELED',            3
),

terminal_facts AS (
    SELECT
        e.delivery_id,
        MIN(sp.priority) AS best_priority
    FROM `portfolio_demo.delivery_status_events` AS e
    JOIN status_priority AS sp
      ON sp.status = e.status          -- non-terminal statuses drop out here
    GROUP BY e.delivery_id
),

latest_ingested AS (
    SELECT delivery_id, status AS latest_status
    FROM `portfolio_demo.delivery_status_events`
    WHERE status IN ('COMPLETED', 'MISSED', 'CANCELED')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY delivery_id
        ORDER BY ingested_ts DESC, event_id DESC
    ) = 1
)

SELECT
    COALESCE(sp.status, 'PENDING')                        AS precedence_outcome,
    COALESCE(l.latest_status, 'PENDING')                  AS latest_ingested_outcome,
    COUNT(*)                                              AS deliveries
FROM window_deliveries AS w
LEFT JOIN terminal_facts  AS t  ON t.delivery_id = w.delivery_id
LEFT JOIN status_priority AS sp ON sp.priority   = t.best_priority
LEFT JOIN latest_ingested AS l  ON l.delivery_id = w.delivery_id
GROUP BY precedence_outcome, latest_ingested_outcome
ORDER BY precedence_outcome, latest_ingested_outcome;


-- ============================================================================
-- DESIGN DECISIONS
--  * Precedence is data, not code: the priority order is a three-row lookup
--    joined in, instead of a CASE expression repeated wherever an outcome is
--    needed. In production it would be a small reference table.
--  * Because the outcome is MIN(priority), it cannot depend on timestamps,
--    duplicates or arrival order -- there is no tie to break.
--  * Pending deliveries inside the window are reported, not hidden -- a rising
--    pending count is usually a feed problem, which case study 12 covers.
--  * The window is 28 COMPLETE local days ending yesterday, so today's
--    half-finished deliveries never drag the rate around during the day.
-- ============================================================================
