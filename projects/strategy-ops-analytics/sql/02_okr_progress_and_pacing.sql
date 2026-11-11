-- ============================================================================
-- 02: OKR Progress, Pacing and Roll-Up
-- Dialect: BigQuery   (runnable: python3 run.py)
-- ============================================================================
-- BUSINESS PROBLEM
-- Each objective has two or three key results with a start value, a target and
-- a weight. Owners post check-ins roughly weekly. Leadership wants one line
-- per objective: how far along is it, is that enough for this point in the
-- quarter, and which ones have gone quiet?
--
-- "62% complete" means nothing without the calendar. A key result at 30% one
-- third of the way through the quarter is on pace; at 30% with two weeks left
-- it is in trouble. Status here is progress RELATIVE to elapsed time.
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema and synthetic data. Not employer code. See NOTICE.md.
-- ============================================================================

WITH params AS (
    SELECT DATE '2026-10-31' AS as_of_date      -- CURRENT_DATE() in production
),

latest_checkin AS (
    SELECT c.kr_id, c.checkin_date, c.current_value
    FROM `stratops_demo.okr_checkins` AS c
    CROSS JOIN params AS p
    WHERE c.checkin_date <= p.as_of_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.kr_id ORDER BY c.checkin_date DESC) = 1
),

kr_scored AS (
    SELECT
        k.org_id,
        k.objective_id,
        k.kr_id,
        k.kr_name,
        k.weight,
        l.checkin_date,
        DATE_DIFF(p.as_of_date, l.checkin_date, DAY) AS days_since_checkin,
        -- (current - start) / (target - start) is direction-agnostic: for a
        -- "reduce" metric both differences are negative and the ratio is still
        -- the share of the distance covered. Clamped to 0-1 so one overshoot
        -- cannot mask a miss elsewhere in the roll-up.
        LEAST(1, GREATEST(0, COALESCE(
            SAFE_DIVIDE(l.current_value - k.start_value, k.target_value - k.start_value), 0)))
                                                             AS progress,
        -- Expected progress is measured at the CHECK-IN date, not today. A
        -- weekly check-in is up to six days old; judging last Monday's number
        -- against today's calendar would mark healthy work as behind.
        SAFE_DIVIDE(DATE_DIFF(COALESCE(l.checkin_date, p.as_of_date), k.quarter_start_date, DAY),
                    DATE_DIFF(k.quarter_end_date, k.quarter_start_date, DAY))
                                                             AS expected_progress,
        SAFE_DIVIDE(DATE_DIFF(p.as_of_date, k.quarter_start_date, DAY),
                    DATE_DIFF(k.quarter_end_date, k.quarter_start_date, DAY))
                                                             AS quarter_elapsed
    FROM `stratops_demo.okr_key_results` AS k
    CROSS JOIN params AS p
    LEFT JOIN latest_checkin AS l          -- a KR nobody has updated must still appear
      ON l.kr_id = k.kr_id
),

kr_status AS (
    SELECT
        *,
        CASE
            WHEN checkin_date IS NULL                    THEN 'NO DATA'
            WHEN days_since_checkin > 14                 THEN 'STALE'
            WHEN progress >= 0.90 * expected_progress    THEN 'ON TRACK'
            WHEN progress >= 0.60 * expected_progress    THEN 'AT RISK'
            ELSE 'OFF TRACK'
        END AS status
    FROM kr_scored
)

SELECT
    o.org_name,
    s.objective_id,
    COUNT(*)                                                         AS key_results,
    -- Dividing by SUM(weight) keeps the roll-up a true weighted average even
    -- when an objective's weights were entered wrong (see 05, check 5).
    ROUND(100 * SAFE_DIVIDE(SUM(s.weight * s.progress), SUM(s.weight)), 1)
                                                                     AS weighted_progress_pct,
    ROUND(100 * MAX(s.quarter_elapsed), 1)                           AS quarter_elapsed_pct,
    COUNTIF(s.status = 'ON TRACK')                                   AS on_track,
    COUNTIF(s.status = 'AT RISK')                                    AS at_risk,
    COUNTIF(s.status = 'OFF TRACK')                                  AS off_track,
    COUNTIF(s.status IN ('STALE', 'NO DATA'))                        AS stale_or_no_data,
    -- An objective is only as healthy as its worst key result
    CASE
        WHEN COUNTIF(s.status = 'OFF TRACK') > 0                 THEN 'OFF TRACK'
        WHEN COUNTIF(s.status IN ('STALE', 'NO DATA')) > 0       THEN 'NEEDS UPDATE'
        WHEN COUNTIF(s.status = 'AT RISK') > 0                   THEN 'AT RISK'
        ELSE 'ON TRACK'
    END                                                              AS objective_status
FROM kr_status AS s
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = s.org_id
GROUP BY o.org_name, s.objective_id
ORDER BY weighted_progress_pct, o.org_name, s.objective_id;

-- ============================================================================
-- DESIGN DECISIONS
--  * Pacing bands (90% / 60% of expected) are parameters of this demo, stated
--    once in one CASE. They assume linear progress; a KR that is back-loaded
--    by design should carry its own expected curve.
--  * STALE outranks the progress bands. A two-week-old number is not evidence
--    of being on track, and reporting it as such is how surprises happen at
--    quarter end.
--  * Progress is clamped per key result BEFORE weighting, so 180% on an easy
--    KR cannot hide 20% on a hard one.
-- ============================================================================
