SELECT county_fips, year, count(*) AS cnt
FROM gold_hazard.county_year_universe
GROUP BY 1,2
HAVING count(*) > 1;
