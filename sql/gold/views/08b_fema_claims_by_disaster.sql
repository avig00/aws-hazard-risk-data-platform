-- Claims aggregated to disaster level (single row per disaster)
CREATE OR REPLACE VIEW gold_hazard._fema_claims_by_disaster AS
SELECT
  cast(disasternumber AS bigint) AS disasternumber,
  sum(coalesce(cast(validregistrations AS double), 0)) AS validregistrations,
  sum(coalesce(cast(totaldamage AS double), 0)) AS totaldamage,
  sum(coalesce(cast(totalapprovedihpamount AS double), 0)) AS totalapprovedihpamount,
  sum(coalesce(cast(repairreplaceamount AS double), 0)) AS repairreplaceamount,
  sum(coalesce(cast(rentalamount AS double), 0)) AS rentalamount,
  sum(coalesce(cast(otherneedsamount AS double), 0)) AS otherneedsamount,
  sum(coalesce(cast(totalinspected AS double), 0)) AS totalinspected
FROM silver_hazard_cleaned.fema_claims_clean
WHERE disasternumber IS NOT NULL
GROUP BY 1;
