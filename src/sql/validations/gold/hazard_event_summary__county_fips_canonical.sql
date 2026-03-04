SELECT COUNT(*) AS failures
FROM (
  SELECT DISTINCT h.county_fips
  FROM {{validation_table_hazard_event_summary}} h
  LEFT JOIN (
    SELECT DISTINCT TRIM(county_fips) AS county_fips
    FROM {{athena_db_silver}}.census_clean
    WHERE county_fips IS NOT NULL
      AND LENGTH(TRIM(county_fips)) = 5
      AND substr(TRIM(county_fips), 1, 2) NOT IN ('00', '03')
      AND substr(TRIM(county_fips), 3, 3) <> '000'
  ) c
    ON h.county_fips = c.county_fips
  WHERE c.county_fips IS NULL
) t;
