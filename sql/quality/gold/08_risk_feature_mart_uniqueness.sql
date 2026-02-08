SELECT county_fips, year, count(*) AS cnt
FROM gold_hazard.risk_feature_mart
GROUP BY 1,2
HAVING count(*) > 1;
