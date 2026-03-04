SELECT COUNT(*) AS failures
FROM (
  SELECT u.county_fips, u.year
  FROM {{validation_table_county_year_universe}} u
  LEFT JOIN {{validation_table_risk_feature_mart}} m
    ON u.county_fips = m.county_fips AND u.year = m.year
  WHERE m.county_fips IS NULL
) t;
