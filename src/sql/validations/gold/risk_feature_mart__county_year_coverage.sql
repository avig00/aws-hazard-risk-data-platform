SELECT COUNT(*) AS failures
FROM (
  SELECT u.county_fips, u.year
  FROM county_year_universe u
  LEFT JOIN risk_feature_mart m
    ON u.county_fips = m.county_fips AND u.year = m.year
  WHERE m.county_fips IS NULL
) t;
