SELECT county_fips, count(*) AS cnt
FROM gold_hazard.county_dim
GROUP BY county_fips
HAVING count(*) > 1;
