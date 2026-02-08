-- NOAA county-year aggregates (rolled up from hazard_event_summary)
CREATE OR REPLACE VIEW gold_hazard._noaa_county_year AS
SELECT
  county_fips,
  year,
  sum(event_count) AS noaa_event_count,
  sum(total_fatalities) AS noaa_total_fatalities,
  sum(total_injuries) AS noaa_total_injuries,
  avg(avg_property_damage) AS noaa_avg_property_damage
FROM gold_hazard.hazard_event_summary
GROUP BY 1,2;
