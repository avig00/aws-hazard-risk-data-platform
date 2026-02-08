-- Gold Table 3: county_dim
-- Purpose: human-readable lookups (county + state)
-- Sources:
--   - NRI provides county + state fields (string), plus county fips
--   - Census provides latest county_name
--
-- Result fields:
--   county_fips, county_name, state, state_fips, county_name_normalized
CREATE TABLE gold_hazard.county_dim
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://aws-hazard-risk-vigamogh-dev/hazard/gold/county_dim/'
) AS
WITH census_latest_name AS (
  SELECT county_fips, county_name
  FROM (
    SELECT
      lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
      county_name,
      acs_year,
      row_number() OVER (
        PARTITION BY lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0')
        ORDER BY acs_year DESC
      ) AS rn
    FROM silver_hazard_cleaned.census_clean
    WHERE county_fips IS NOT NULL
      AND county_name IS NOT NULL
      AND acs_year IS NOT NULL
  )
  WHERE rn = 1
),
nri_base AS (
  SELECT
    lpad(regexp_replace(coalesce(county_fips, countyfips), '[^0-9]', ''), 5, '0') AS county_fips,
    -- NRI has "county" and "state" strings
    max(state) AS state,
    max(statefips) AS state_fips_str,
    max(county) AS county_name_nri
  FROM silver_hazard_cleaned.nri_scores_clean
  WHERE coalesce(county_fips, countyfips) IS NOT NULL
  GROUP BY 1
)
SELECT
  n.county_fips,
  coalesce(c.county_name, n.county_name_nri) AS county_name,
  n.state AS state,
  -- statefips is a string in NRI; normalize to 2-char; avoid "00" for null/blank
  CASE
    WHEN n.state_fips_str IS NULL OR trim(n.state_fips_str) = '' THEN NULL
    ELSE lpad(regexp_replace(n.state_fips_str, '[^0-9]', ''), 2, '0')
  END AS state_fips,
  lower(trim(coalesce(c.county_name, n.county_name_nri))) AS county_name_normalized
FROM nri_base n
LEFT JOIN census_latest_name c
  ON n.county_fips = c.county_fips;
