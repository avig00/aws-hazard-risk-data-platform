SELECT COUNT(*) AS failures
FROM (
  SELECT county_fips, year, hazard_type
  FROM hazard_event_summary
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
) t;
