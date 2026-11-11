-- ============================================================================
-- 04: Headcount What-If Scenarios vs. Plan
-- Dialect: BigQuery   (runnable: python3 run.py -- reads the tables built by 01)
-- ============================================================================
-- BUSINESS PROBLEM
-- "If nothing changes, where do we land against plan? What if we freeze
-- hiring? What if we fix retention?" Leadership needs the same projection run
-- under several assumptions and laid side by side, with the assumptions
-- visible -- not four spreadsheets with four hidden formulas.
--
-- MODEL
-- Each month an org loses a fixed share of its people (attrition rate a) and
-- gains a fixed number of hires (h). Starting from headcount H0, after n
-- months:
--
--     H(n) = H0 * (1 - a)^n  +  h * (1 - (1 - a)^n) / a
--
-- The closed form lets one query project any horizon without a loop. Rates
-- come from each org's own last six months, then each scenario scales them.
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema and synthetic data. Not employer code. See NOTICE.md.
-- ============================================================================

WITH params AS (
    SELECT DATE '2026-10-31' AS as_of_date,
           DATE '2026-05-01' AS trend_start        -- six-month observation window
),

scenarios AS (
    SELECT 1 AS sort_key, 'Current trend' AS scenario, 1.00 AS hiring_multiplier, 1.00 AS attrition_multiplier
    UNION ALL SELECT 2, 'Hiring freeze',     0.00, 1.00
    UNION ALL SELECT 3, 'Hiring +25%',       1.25, 1.00
    UNION ALL SELECT 4, 'Retention program', 1.00, 0.75
    UNION ALL SELECT 5, 'Hiring +25% and retention program', 1.25, 0.75
),

current_headcount AS (
    SELECT h.org_id, h.headcount AS h0
    FROM `stratops_demo.month_end_headcount` AS h
    CROSS JOIN params AS p
    WHERE h.month_end = p.as_of_date
),

observed AS (
    SELECT
        e.org_id,
        COUNTIF(e.event_type = 'HIRE') / 6.0         AS monthly_hires,
        COUNTIF(e.event_type = 'TERMINATION') / 6.0  AS monthly_terminations
    FROM `stratops_demo.workforce_events_clean` AS e
    CROSS JOIN params AS p
    WHERE e.effective_date BETWEEN p.trend_start AND p.as_of_date
    GROUP BY e.org_id
),

avg_headcount AS (
    SELECT h.org_id, AVG(h.headcount) AS avg_headcount
    FROM `stratops_demo.month_end_headcount` AS h
    CROSS JOIN params AS p
    WHERE h.month_end BETWEEN p.trend_start AND p.as_of_date
    GROUP BY h.org_id
),

assumptions AS (
    SELECT
        c.org_id,
        s.sort_key,
        s.scenario,
        c.h0,
        COALESCE(ob.monthly_hires, 0) * s.hiring_multiplier                 AS h,
        -- floor the rate so the closed form never divides by zero
        GREATEST(SAFE_DIVIDE(COALESCE(ob.monthly_terminations, 0), a.avg_headcount)
                 * s.attrition_multiplier, 0.0001)                          AS a
    FROM current_headcount AS c
    CROSS JOIN scenarios AS s
    LEFT JOIN observed      AS ob ON ob.org_id = c.org_id
    LEFT JOIN avg_headcount AS a  ON a.org_id  = c.org_id
),

projected AS (
    SELECT
        x.org_id,
        x.sort_key,
        x.scenario,
        x.h0,
        LAST_DAY(DATE_ADD(DATE '2026-10-01', INTERVAL n MONTH), MONTH)       AS projection_date,
        x.h0 * POW(1 - x.a, n) + x.h * (1 - POW(1 - x.a, n)) / x.a          AS projected_headcount
    FROM assumptions AS x
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 5)) AS n      -- Nov 2026 .. Mar 2027
)

SELECT
    o.org_name,
    p.scenario,
    p.h0                                                                    AS headcount_today,
    MAX(IF(p.projection_date = DATE '2026-12-31', CAST(ROUND(p.projected_headcount) AS INT64), NULL)) AS projected_dec_31,
    MAX(IF(pl.quarter_end_date = DATE '2026-12-31', pl.planned_headcount, NULL))       AS plan_dec_31,
    MAX(IF(p.projection_date = DATE '2026-12-31',
           CAST(ROUND(p.projected_headcount) AS INT64) - pl.planned_headcount, NULL))                 AS gap_dec_31,
    MAX(IF(p.projection_date = DATE '2027-03-31', CAST(ROUND(p.projected_headcount) AS INT64), NULL)) AS projected_mar_31,
    MAX(IF(pl.quarter_end_date = DATE '2027-03-31', pl.planned_headcount, NULL))       AS plan_mar_31,
    MAX(IF(p.projection_date = DATE '2027-03-31',
           CAST(ROUND(p.projected_headcount) AS INT64) - pl.planned_headcount, NULL))                 AS gap_mar_31
FROM projected AS p
JOIN `stratops_demo.orgs` AS o
  ON o.org_id = p.org_id
JOIN `stratops_demo.headcount_plan` AS pl
  ON pl.org_id = p.org_id
 AND pl.quarter_end_date = p.projection_date           -- keeps quarter ends only
GROUP BY o.org_name, p.scenario, p.h0, p.sort_key
ORDER BY o.org_name, p.sort_key;

-- ============================================================================
-- DESIGN DECISIONS
--  * Scenarios are rows, not copies of the query. Adding a fifth scenario is
--    one more line in the scenarios CTE and every org is re-projected.
--  * Each org's rates come from its own recent history, so "current trend"
--    means that org's trend, not a network average applied to everyone.
--  * This is a deliberately simple model: constant rates, no seasonality, no
--    hiring lead time. It is meant to size a gap and compare options, and it
--    says so. Six months of history on a 40-person team is a noisy rate.
-- ============================================================================
