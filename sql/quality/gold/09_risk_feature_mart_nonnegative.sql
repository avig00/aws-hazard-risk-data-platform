SELECT *
FROM gold_hazard.risk_feature_mart
WHERE noaa_event_count < 0
   OR noaa_total_fatalities < 0
   OR noaa_total_injuries < 0
   OR noaa_avg_property_damage < 0
   OR fema_valid_registrations < 0
   OR fema_total_damage < 0
   OR fema_total_approved_ihp_amount < 0
   OR fema_declaration_count < 0
LIMIT 50;
