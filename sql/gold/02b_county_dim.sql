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
  SELECT
    county_fips,
    county_name AS county_name_raw
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
    lpad(regexp_replace(county_fips, '[^0-9]', ''), 5, '0') AS county_fips,
    -- NRI has "county" and "state" strings
    max(state) AS state,
    max(statefips) AS state_fips_str,
    trim(regexp_replace(max(county), ',\\s*[^,]+$', '')) AS county_name_nri
  FROM silver_hazard_cleaned.nri_scores_clean
  WHERE county_fips IS NOT NULL
    AND length(trim(county_fips)) = 5
    AND substr(trim(county_fips), 1, 2) NOT IN ('00', '03')
    AND substr(trim(county_fips), 3, 3) <> '000'
  GROUP BY 1
)
,
resolved_names AS (
  SELECT
    n.county_fips,
    n.state,
    n.state_fips_str,
    trim(regexp_replace(regexp_replace(n.county_name_nri, ',\\s*Connecticut$', ''), ',\\s*Conneccticut$', '')) AS county_name_nri,
    CASE
      WHEN c.county_name_raw IS NOT NULL
        AND n.state IS NOT NULL
        AND regexp_like(c.county_name_raw, concat(',\\s*', n.state, '$'))
      THEN trim(regexp_replace(c.county_name_raw, concat(',\\s*', n.state, '$'), ''))
      WHEN c.county_name_raw IS NOT NULL
        AND n.state = 'Connecticut'
        AND regexp_like(c.county_name_raw, ',\\s*Conneccticut$')
      THEN trim(regexp_replace(c.county_name_raw, ',\\s*Conneccticut$', ''))
      ELSE NULL
    END AS county_name_census
  FROM nri_base n
  LEFT JOIN census_latest_name c
    ON n.county_fips = c.county_fips
),
final_names AS (
  SELECT
    r.county_fips,
    r.state,
    r.state_fips_str,
    CASE
      WHEN r.county_fips = '02020' THEN 'Anchorage Municipality'
      WHEN r.county_fips = '02013' THEN 'Aleutians East Borough'
      WHEN r.county_fips = '02016' THEN 'Aleutians West Census Area'
      WHEN r.county_fips = '02050' THEN 'Bethel Census Area'
      WHEN r.county_fips = '02060' THEN 'Bristol Bay Borough'
      WHEN r.county_fips = '02063' THEN 'Chugach Census Area'
      WHEN r.county_fips = '02066' THEN 'Copper River Census Area'
      WHEN r.county_fips = '02068' THEN 'Denali Borough'
      WHEN r.county_fips = '02070' THEN 'Dillingham Census Area'
      WHEN r.county_fips = '02090' THEN 'Fairbanks North Star Borough'
      WHEN r.county_fips = '02100' THEN 'Haines Borough'
      WHEN r.county_fips = '02105' THEN 'Hoonah-Angoon Census Area'
      WHEN r.county_fips = '02110' THEN 'Juneau City and Borough'
      WHEN r.county_fips = '02122' THEN 'Kenai Peninsula Borough'
      WHEN r.county_fips = '02130' THEN 'Ketchikan Gateway Borough'
      WHEN r.county_fips = '02150' THEN 'Kodiak Island Borough'
      WHEN r.county_fips = '02158' THEN 'Kusilvak Census Area'
      WHEN r.county_fips = '02164' THEN 'Lake and Peninsula Borough'
      WHEN r.county_fips = '02170' THEN 'Matanuska-Susitna Borough'
      WHEN r.county_fips = '02180' THEN 'Nome Census Area'
      WHEN r.county_fips = '02185' THEN 'North Slope Borough'
      WHEN r.county_fips = '02188' THEN 'Northwest Arctic Borough'
      WHEN r.county_fips = '02195' THEN 'Petersburg Borough'
      WHEN r.county_fips = '02198' THEN 'Prince of Wales-Hyder Census Area'
      WHEN r.county_fips = '02220' THEN 'Sitka City and Borough'
      WHEN r.county_fips = '02230' THEN 'Skagway Municipality'
      WHEN r.county_fips = '02240' THEN 'Southeast Fairbanks Census Area'
      WHEN r.county_fips = '02275' THEN 'Wrangell City and Borough'
      WHEN r.county_fips = '02282' THEN 'Yakutat City and Borough'
      WHEN r.county_fips = '02290' THEN 'Yukon-Koyukuk Census Area'
      ELSE coalesce(nullif(r.county_name_nri, ''), nullif(r.county_name_census, ''))
    END AS county_name_final_raw
  FROM resolved_names r
)
SELECT
  f.county_fips,
  CASE
    WHEN f.state = 'Connecticut'
    THEN trim(regexp_replace(f.county_name_final_raw, ' +Connecticut$', ''))
    ELSE f.county_name_final_raw
  END AS county_name,
  f.state,
  -- statefips is a string in NRI; normalize to 2-char; avoid "00" for null/blank
  CASE
    WHEN f.state_fips_str IS NULL OR trim(f.state_fips_str) = '' THEN NULL
    ELSE lpad(regexp_replace(f.state_fips_str, '[^0-9]', ''), 2, '0')
  END AS state_fips,
  lower(trim(
    CASE
      WHEN f.state = 'Connecticut'
      THEN trim(regexp_replace(f.county_name_final_raw, ' +Connecticut$', ''))
      ELSE f.county_name_final_raw
    END
  )) AS county_name_normalized
FROM final_names f;
