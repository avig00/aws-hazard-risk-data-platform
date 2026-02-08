SELECT *
FROM gold_hazard._fema_claims_county_year
WHERE fema_valid_registrations < 0
   OR fema_total_damage < 0
   OR fema_total_approved_ihp_amount < 0
   OR fema_repair_replace_amount < 0
   OR fema_rental_amount < 0
   OR fema_other_needs_amount < 0
   OR fema_total_inspected < 0
LIMIT 50;
