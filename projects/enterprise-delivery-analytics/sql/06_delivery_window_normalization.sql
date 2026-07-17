-- ============================================================================
-- Case Study 6: Premium Delivery-Window Normalization & Conditional
--               Archive Filtering
-- Dialect: SQL Server (T-SQL)
-- ============================================================================
-- BUSINESS PROBLEM
-- Delivery schedules contained general activity windows plus premium two- and
-- four-hour windows. Reporting needed to:
--   * Default non-premium generated windows to a standard '6AM - 8PM' label
--   * Preserve and format premium / explicitly configured windows
--   * Convert 24-hour times to 12-hour labels (midnight/noon edge cases!)
--   * Span recent history AND future schedules
--   * Apply "NumberOfStops > 0" ONLY to historical archive schedules
--
-- The hard part is BUSINESS-RULE PRECEDENCE: a broadly-placed stop-count
-- filter would silently remove future or non-archive schedules and change the
-- meaning of the report.
--
-- NOTE: sanitized reconstruction — table and column names are generalized.
-- ============================================================================

SELECT
    re.schedule_key,
    re.earliest_start_date,
    re.number_of_stops,

    CASE
        -- Non-premium generated windows collapse to the standard label.
        -- Premium 2- and 4-hour windows fall through to real formatting.
        WHEN ISNULL(s.generate_activity_window_ind, 0) = 0
         AND DATEDIFF(HOUR, re.stop_window_open, re.stop_window_close) NOT IN (2, 4)
        THEN '6AM - 8PM'

        ELSE
            -- 24h -> 12h conversion; midnight must become 12AM, never 0AM
            CAST(
                CASE
                    WHEN DATEPART(HOUR, re.stop_window_open) = 0  THEN 12
                    WHEN DATEPART(HOUR, re.stop_window_open) > 12
                        THEN DATEPART(HOUR, re.stop_window_open) - 12
                    ELSE DATEPART(HOUR, re.stop_window_open)
                END AS VARCHAR(2)
            )
            + CASE WHEN DATEPART(HOUR, re.stop_window_open) >= 12 THEN 'PM' ELSE 'AM' END
            + ' - '
            + CAST(
                CASE
                    WHEN DATEPART(HOUR, re.stop_window_close) = 0  THEN 12
                    WHEN DATEPART(HOUR, re.stop_window_close) > 12
                        THEN DATEPART(HOUR, re.stop_window_close) - 12
                    ELSE DATEPART(HOUR, re.stop_window_close)
                END AS VARCHAR(2)
            )
            + CASE WHEN DATEPART(HOUR, re.stop_window_close) >= 12 THEN 'PM' ELSE 'AM' END
    END AS stop_window_label

FROM dbo.route_execution AS re
LEFT JOIN dbo.schedule_configuration AS s
  ON s.schedule_id = re.schedule_id

-- Explicit casts prevent date/int type clashes and time-of-day surprises
WHERE CAST(re.earliest_start_date AS DATE)
      BETWEEN DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
          AND DATEADD(DAY, 30, CAST(GETDATE() AS DATE))

  -- The archive rule is parenthesized so NumberOfStops > 0 applies ONLY to
  -- past archive schedules — live and future schedules are untouched.
  AND (
        (
            re.schedule_key LIKE '%ARCHIVE%'
            AND CAST(re.earliest_start_date AS DATE) < CAST(GETDATE() AS DATE)
            AND re.number_of_stops > 0
        )
        OR re.schedule_key NOT LIKE '%ARCHIVE%'
      );

-- ============================================================================
-- DESIGN DECISIONS
--   * Boolean precedence made explicit with parentheses — the #1 way filters
--     silently change a report's meaning
--   * Premium durations preserved rather than overwritten by the default label
--   * Midnight converts to 12AM, not 0AM
--   * If multiple reports need identical window labels, this logic belongs in
--     a reusable view or function rather than copy-pasted per report
-- ============================================================================
