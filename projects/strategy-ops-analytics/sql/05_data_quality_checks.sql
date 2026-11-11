-- ============================================================================
-- 05: Data-Quality Scorecard
-- Dialect: BigQuery   (runnable: python3 run.py)
-- ============================================================================
-- BUSINESS PROBLEM
-- Every number in 01-04 rests on source data that people key in by hand. This
-- scorecard runs with the reports and answers "can these numbers be trusted
-- today?" in one table: what was checked, how many rows failed, and which
-- report each failure would distort.
--
-- Each check returns a count, so the scorecard can be trended over time and a
-- threshold can gate publication.
--
-- PROVENANCE: original SQL written for this portfolio against an invented
-- schema and synthetic data. Not employer code. See NOTICE.md.
-- ============================================================================

SELECT 1 AS check_id, 'Duplicate workforce event records' AS check_name, 'MEDIUM' AS severity,
       (SELECT COUNT(*) FROM (
            SELECT employee_id, event_type, org_id, effective_date
            FROM `stratops_demo.workforce_events`
            GROUP BY employee_id, event_type, org_id, effective_date
            HAVING COUNT(*) > 1)) AS failing_rows,
       'Headcount and attrition double-count unless deduplicated (handled in 01)' AS impact

UNION ALL
SELECT 2, 'Workforce events for an unknown org', 'HIGH',
       (SELECT COUNT(*)
        FROM `stratops_demo.workforce_events` AS e
        LEFT JOIN `stratops_demo.orgs` AS o ON o.org_id = e.org_id
        WHERE o.org_id IS NULL),
       'These people are missing from every headcount figure until the org is fixed'

UNION ALL
SELECT 3, 'Termination dated before the employee was hired', 'HIGH',
       (SELECT COUNT(*)
        FROM `stratops_demo.workforce_events` AS t
        JOIN (SELECT employee_id, MIN(effective_date) AS hire_date
              FROM `stratops_demo.workforce_events`
              WHERE event_type = 'HIRE'
              GROUP BY employee_id) AS h
          ON h.employee_id = t.employee_id
        WHERE t.event_type = 'TERMINATION'
          AND t.effective_date < h.hire_date),
       'Would inflate attrition; excluded from the clean layer in 01'

UNION ALL
SELECT 4, 'Events recorded more than 10 days after they took effect', 'LOW',
       (SELECT COUNT(*)
        FROM `stratops_demo.workforce_events`
        WHERE DATE_DIFF(DATE(recorded_ts), effective_date, DAY) > 10),
       'Past month-end headcount is restated after the fact; publish with a refresh date'

UNION ALL
SELECT 5, 'Objectives whose key-result weights do not sum to 1', 'MEDIUM',
       (SELECT COUNT(*) FROM (
            SELECT objective_id
            FROM `stratops_demo.okr_key_results`
            GROUP BY objective_id
            HAVING ABS(SUM(weight) - 1) > 0.001)),
       'Roll-up is normalized in 02, but the owner intended something else'

UNION ALL
SELECT 6, 'Key results with no check-in in the last 14 days', 'MEDIUM',
       (SELECT COUNT(*)
        FROM `stratops_demo.okr_key_results` AS k
        LEFT JOIN (SELECT kr_id, MAX(checkin_date) AS last_checkin
                   FROM `stratops_demo.okr_checkins`
                   WHERE checkin_date <= DATE '2026-10-31'
                   GROUP BY kr_id) AS c
          ON c.kr_id = k.kr_id
        WHERE c.last_checkin IS NULL
           OR DATE_DIFF(DATE '2026-10-31', c.last_checkin, DAY) > 14),
       'Objective status is reported as NEEDS UPDATE rather than guessed'

UNION ALL
SELECT 7, 'Key results whose target contradicts their direction', 'HIGH',
       (SELECT COUNT(*)
        FROM `stratops_demo.okr_key_results`
        WHERE (direction = 'INCREASE' AND target_value <= start_value)
           OR (direction = 'DECREASE' AND target_value >= start_value)),
       'Progress would be computed backwards'

UNION ALL
SELECT 8, 'Missing days in daily KPI series', 'MEDIUM',
       (SELECT COALESCE(SUM(gap_days), 0) FROM (
            SELECT DATE_DIFF(metric_date,
                             LAG(metric_date) OVER (PARTITION BY org_id, metric_name ORDER BY metric_date),
                             DAY) - 1 AS gap_days
            FROM `stratops_demo.kpi_daily`)
        WHERE gap_days > 0),
       'Baselines in 03 shift by a day; the gap is also raised as its own alert'

UNION ALL
SELECT 9, 'Orgs with no headcount plan for the current quarter', 'HIGH',
       (SELECT COUNT(*)
        FROM `stratops_demo.orgs` AS o
        LEFT JOIN `stratops_demo.headcount_plan` AS p
          ON p.org_id = o.org_id AND p.quarter_end_date = DATE '2026-12-31'
        WHERE p.org_id IS NULL),
       'Plan-vs-actual and the scenario gaps in 04 would silently drop the org'

ORDER BY check_id;
