SELECT COUNT(*) AS failures
FROM (
  SELECT county_fips, year
  FROM risk_feature_mart
  GROUP BY 1,2
  HAVING COUNT(*) > 1
) t;
