SELECT county_fips, year, hazard_type, hazard_category, count(*) AS cnt
FROM gold_hazard.hazard_event_summary
GROUP BY 1,2,3,4
HAVING count(*) > 1;
