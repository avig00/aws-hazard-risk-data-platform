-- County counts per disaster-year (for equal allocation)
CREATE OR REPLACE VIEW gold_hazard._fema_disaster_county_counts AS
SELECT
  disasternumber,
  year,
  count(DISTINCT county_fips) AS county_cnt
FROM gold_hazard._fema_disaster_county_map
GROUP BY 1,2;
