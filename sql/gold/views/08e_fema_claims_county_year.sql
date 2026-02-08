-- Final county-year claims features (allocated)
CREATE OR REPLACE VIEW gold_hazard._fema_claims_county_year AS
SELECT
  m.county_fips,
  m.year,
  sum(c.validregistrations / nullif(cc.county_cnt, 0)) AS fema_valid_registrations,
  sum(c.totaldamage / nullif(cc.county_cnt, 0)) AS fema_total_damage,
  sum(c.totalapprovedihpamount / nullif(cc.county_cnt, 0)) AS fema_total_approved_ihp_amount,
  sum(c.repairreplaceamount / nullif(cc.county_cnt, 0)) AS fema_repair_replace_amount,
  sum(c.rentalamount / nullif(cc.county_cnt, 0)) AS fema_rental_amount,
  sum(c.otherneedsamount / nullif(cc.county_cnt, 0)) AS fema_other_needs_amount,
  sum(c.totalinspected / nullif(cc.county_cnt, 0)) AS fema_total_inspected
FROM gold_hazard._fema_disaster_county_map m
JOIN gold_hazard._fema_claims_by_disaster c
  ON m.disasternumber = c.disasternumber
JOIN gold_hazard._fema_disaster_county_counts cc
  ON m.disasternumber = cc.disasternumber
 AND m.year = cc.year
GROUP BY 1,2;
