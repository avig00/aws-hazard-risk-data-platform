SELECT COUNT(*) AS failures
FROM {{validation_table_hazard_event_summary}}
WHERE event_count < 0
   OR total_fatalities < 0
   OR total_injuries < 0
   OR avg_property_damage < 0;
