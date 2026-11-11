-- ============================================================================
-- 01: Month-End Headcount from Event History, Plan vs. Actual, Attrition
-- Dialect: BigQuery   (runnable: python3 run.py)
-- ============================================================================
-- BUSINESS PROBLEM
-- Leadership asks "how many people do we have against plan, and where are we
-- losing them?" There is no headcount table. There is a log of workforce
-- events (hire, transfer, termination), recorded late, occasionally twice, and
-- sometimes pointing at an org that does not exist.
--
-- Headcount on a date is a POINT-IN-TIME question: for every employee, what
-- was their most recent event on or before that date? This file builds that
-- once, as a curated table the other analyses read, then answers the
-- leadership questions from it.
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema and synthetic data. Not employer code. See NOTICE.md.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Curated layer 1: one clean row per real-world event
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `stratops_demo.workforce_events_clean` AS
WITH deduplicated AS (
    SELECT
        e.employee_id,
        e.event_type,
        e.org_id,
        e.effective_date,
        MIN(e.recorded_ts) AS first_recorded_ts     -- a re-keyed duplicate is one event
    FROM `stratops_demo.workforce_events` AS e
    -- Inner join on purpose: an event for an unknown org cannot be attributed.
    JOIN `stratops_demo.orgs` AS o
      ON o.org_id = e.org_id
    GROUP BY e.employee_id, e.event_type, e.org_id, e.effective_date
)

SELECT
    employee_id,
    event_type,
    org_id,
    effective_date,
    first_recorded_ts
FROM deduplicated
-- A termination dated before the person's first hire is a keying error, not
-- a departure. Both exclusions are COUNTED in 05_data_quality_checks.sql, so
-- they are visible rather than silent.
QUALIFY NOT (
    event_type = 'TERMINATION'
    AND effective_date < MIN(IF(event_type = 'HIRE', effective_date, NULL))
                             OVER (PARTITION BY employee_id)
);

-- ----------------------------------------------------------------------------
-- Curated layer 2: headcount by org at every month end
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `stratops_demo.month_end_headcount` AS
WITH month_ends AS (
    SELECT LAST_DAY(month_start, MONTH) AS month_end
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2025-01-01', DATE '2026-10-31', INTERVAL 1 MONTH))
         AS month_start
),

state_at_month_end AS (
    SELECT
        m.month_end,
        e.employee_id,
        e.event_type,
        e.org_id
    FROM month_ends AS m
    JOIN `stratops_demo.workforce_events_clean` AS e
      ON e.effective_date <= m.month_end        -- effective date, not recorded date:
                                                -- a hire keyed in late still started on time
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY m.month_end, e.employee_id
        ORDER BY e.effective_date DESC, e.first_recorded_ts DESC
    ) = 1
)

SELECT
    month_end,
    org_id,
    COUNT(*) AS headcount
FROM state_at_month_end
WHERE event_type <> 'TERMINATION'               -- last known state is "employed here"
GROUP BY month_end, org_id;

-- ----------------------------------------------------------------------------
-- Q1. Plan vs. actual at each completed quarter end of 2026
-- ----------------------------------------------------------------------------
SELECT
    o.function_name,
    o.org_name,
    p.quarter_end_date,
    p.planned_headcount,
    h.headcount                                              AS actual_headcount,
    h.headcount - p.planned_headcount                        AS variance,
    ROUND(100 * SAFE_DIVIDE(h.headcount - p.planned_headcount, p.planned_headcount), 1)
                                                             AS variance_pct
FROM `stratops_demo.headcount_plan` AS p
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = p.org_id
-- LEFT JOIN: a planned org with nobody in it must show as a gap, not vanish
LEFT JOIN `stratops_demo.month_end_headcount` AS h
  ON h.org_id = p.org_id
 AND h.month_end = p.quarter_end_date
WHERE p.quarter_end_date BETWEEN DATE '2026-01-01' AND DATE '2026-10-31'
ORDER BY p.quarter_end_date, variance_pct, o.org_name;

-- ----------------------------------------------------------------------------
-- Q2. Annualized attrition: last 6 months vs. the 6 months before
-- Rate = terminations / average month-end headcount, scaled to a year.
-- ----------------------------------------------------------------------------
WITH periods AS (
    SELECT 'last_6_months'  AS period, DATE '2026-05-01' AS period_start, DATE '2026-10-31' AS period_end
    UNION ALL
    SELECT 'prior_6_months',           DATE '2025-11-01',                 DATE '2026-04-30'
),

terminations AS (
    SELECT p.period, e.org_id, COUNT(*) AS terminations
    FROM periods AS p
    JOIN `stratops_demo.workforce_events_clean` AS e
      ON e.event_type = 'TERMINATION'
     AND e.effective_date BETWEEN p.period_start AND p.period_end
    GROUP BY p.period, e.org_id
),

avg_headcount AS (
    SELECT p.period, h.org_id, AVG(h.headcount) AS avg_headcount
    FROM periods AS p
    JOIN `stratops_demo.month_end_headcount` AS h
      ON h.month_end BETWEEN p.period_start AND p.period_end
    GROUP BY p.period, h.org_id
)

SELECT
    o.org_name,
    ROUND(MAX(IF(a.period = 'prior_6_months',
                 100 * 2 * SAFE_DIVIDE(COALESCE(t.terminations, 0), a.avg_headcount), NULL)), 1)
                                                             AS prior_6m_annualized_attrition_pct,
    ROUND(MAX(IF(a.period = 'last_6_months',
                 100 * 2 * SAFE_DIVIDE(COALESCE(t.terminations, 0), a.avg_headcount), NULL)), 1)
                                                             AS last_6m_annualized_attrition_pct,
    ROUND(MAX(IF(a.period = 'last_6_months',
                 100 * 2 * SAFE_DIVIDE(COALESCE(t.terminations, 0), a.avg_headcount), NULL))
        - MAX(IF(a.period = 'prior_6_months',
                 100 * 2 * SAFE_DIVIDE(COALESCE(t.terminations, 0), a.avg_headcount), NULL)), 1)
                                                             AS change_pts
FROM avg_headcount AS a
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = a.org_id
LEFT JOIN terminations AS t
  ON t.org_id = a.org_id AND t.period = a.period
GROUP BY o.org_name
ORDER BY change_pts DESC;

-- ----------------------------------------------------------------------------
-- Q3. The same comparison one level up: plan vs. actual by function
-- A VP owns a function, not an org. Headcount is summed to the function
-- FIRST and the percentage taken afterwards; averaging the orgs' percentages
-- would weight a 60-person team the same as a 200-person one.
-- ----------------------------------------------------------------------------
SELECT
    o.function_name,
    SUM(p.planned_headcount)                                 AS planned_headcount,
    SUM(COALESCE(h.headcount, 0))                            AS actual_headcount,
    SUM(COALESCE(h.headcount, 0)) - SUM(p.planned_headcount) AS variance,
    ROUND(100 * SAFE_DIVIDE(SUM(COALESCE(h.headcount, 0)) - SUM(p.planned_headcount),
                            SUM(p.planned_headcount)), 1)    AS variance_pct
FROM `stratops_demo.headcount_plan` AS p
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = p.org_id
LEFT JOIN `stratops_demo.month_end_headcount` AS h
  ON h.org_id = p.org_id
 AND h.month_end = p.quarter_end_date
WHERE p.quarter_end_date = DATE '2026-09-30'
GROUP BY o.function_name
ORDER BY variance_pct;

-- ============================================================================
-- DESIGN DECISIONS
--  * Headcount is derived, never stored by hand. One definition (last event on
--    or before the date) serves every month end, so two reports cannot
--    disagree about what "headcount" means.
--  * Effective date decides history; recorded time only breaks ties. Late data
--    entry therefore corrects past month ends on the next run instead of
--    showing up as a false spike in the month it was typed.
--  * Attrition uses average headcount as the denominator. Dividing by the
--    period-end figure overstates the rate for a shrinking team.
-- ============================================================================
