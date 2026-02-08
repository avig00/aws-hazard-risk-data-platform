-- NOAA county-year aggregates by hazard_group (agent-friendly grouping)
CREATE OR REPLACE VIEW gold_hazard._noaa_county_year_by_group AS
SELECT
  s.county_fips,
  s.year,
  m.hazard_group,
  sum(s.event_count) AS event_count,
  sum(s.total_fatalities) AS total_fatalities,
  sum(s.total_injuries) AS total_injuries,
  avg(s.avg_property_damage) AS avg_property_damage
FROM gold_hazard.hazard_event_summary s
LEFT JOIN gold_hazard.hazard_type_map m
  ON s.hazard_type = m.event_type
GROUP BY 1,2,3;
