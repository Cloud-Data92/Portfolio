-- ============================================================================
-- Case Study 3: Geospatial Store & Facility Serviceability Ranking
-- Dialect: BigQuery (GIS functions)
-- ============================================================================
-- BUSINESS PROBLEM
-- Delivery planning and facility integration required knowing which stores were
-- geographically closest to a facility or service point — but "closest" alone
-- is not the answer. The logic had to:
--   * Calculate real distance from latitude/longitude
--   * Keep candidates within the same delivery market
--   * Exclude the origin itself and non-store locations (e.g. DCs)
--   * Apply a maximum service radius
--   * Rank nearest stores deterministically (stable ties)
--   * Return both row-level top-N detail and an aggregated summary
-- Supports facility launches, ZIP/service-area design, routing assumptions,
-- market coverage analysis, and exception investigation.
--
-- NOTE: sanitized reconstruction — `portfolio_demo` replaces production names.
-- ============================================================================

WITH facilities AS (
    SELECT
        facility_id,
        delivery_market,
        ST_GEOGPOINT(longitude, latitude) AS facility_geog
    FROM `portfolio_demo.facility_dimension`
    WHERE is_active = TRUE
),

stores AS (
    SELECT
        store_id,
        delivery_market,
        ST_GEOGPOINT(longitude, latitude) AS store_geog
    FROM `portfolio_demo.store_dimension`
    WHERE is_active = TRUE
      AND is_distribution_center = FALSE      -- operational eligibility, not just geography
),

candidate_distances AS (
    SELECT
        f.facility_id,
        f.delivery_market,
        s.store_id,

        -- ST_DISTANCE returns meters; convert to statute miles
        ST_DISTANCE(f.facility_geog, s.store_geog) / 1609.344 AS distance_miles

    FROM facilities AS f
    JOIN stores AS s
      ON s.delivery_market = f.delivery_market  -- same-market constraint
     AND CAST(s.store_id AS STRING) <> CAST(f.facility_id AS STRING)

    -- Spatial pre-filter: prune the candidate space before ranking
    WHERE ST_DWITHIN(f.facility_geog, s.store_geog, 300 * 1609.344)
),

ranked AS (
    SELECT
        facility_id,
        delivery_market,
        store_id,
        distance_miles,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id
            ORDER BY distance_miles, store_id   -- store_id breaks distance ties deterministically
        ) AS distance_rank
    FROM candidate_distances
),

top_three AS (
    SELECT * FROM ranked WHERE distance_rank <= 3
)

SELECT
    facility_id,
    delivery_market,

    STRING_AGG(
        CAST(store_id AS STRING), ', ' ORDER BY distance_rank
    ) AS closest_store_list,

    ARRAY_AGG(
        STRUCT(
            distance_rank AS rank,
            store_id,
            ROUND(distance_miles, 2) AS distance_miles
        )
        ORDER BY distance_rank
    ) AS closest_store_detail,

    ROUND(MIN(distance_miles), 2) AS nearest_store_miles,
    ROUND(MAX(distance_miles), 2) AS third_nearest_store_miles

FROM top_three
GROUP BY facility_id, delivery_market
ORDER BY delivery_market, facility_id;


-- ============================================================================
-- VARIATION: ZIP-to-store serviceability
-- Assigns each ZIP to its nearest eligible store, but only when the ZIP falls
-- within that store's service radius — coverage gaps surface as missing ZIPs.
-- ============================================================================

WITH store_zip_candidates AS (
    SELECT
        z.zip_code,
        s.store_id,
        s.delivery_market,
        s.service_radius_miles,
        ST_DISTANCE(z.zip_geog, s.store_geog) / 1609.344 AS distance_miles
    FROM `portfolio_demo.zip_dimension` AS z
    JOIN `portfolio_demo.store_dimension` AS s
      ON z.delivery_market = s.delivery_market
    WHERE s.is_distribution_center = FALSE
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY zip_code
            ORDER BY distance_miles, store_id
        ) AS nearest_rank
    FROM store_zip_candidates
    WHERE distance_miles <= service_radius_miles
)

SELECT
    zip_code,
    store_id AS assigned_store,
    ROUND(distance_miles, 2) AS distance_miles
FROM ranked
WHERE nearest_rank = 1;

-- ============================================================================
-- DESIGN DECISIONS
--   * Geography objects built ONCE in dimension CTEs, not per-expression
--   * Same-market joins prevent geographically-close but operationally-invalid
--     assignments
--   * ST_DWITHIN prunes pairs before the expensive full ranking
--   * Deterministic tie-breaking (distance, then store_id) keeps results stable
--   * ARRAY_AGG preserves structured detail; STRING_AGG + MIN/MAX support
--     flat reporting from the same pass
-- ============================================================================
