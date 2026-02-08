WITH claims AS (
  SELECT DISTINCT cast(disasternumber AS bigint) AS disasternumber
  FROM silver_hazard_cleaned.fema_claims_clean
  WHERE disasternumber IS NOT NULL
),
decls AS (
  SELECT DISTINCT cast(disasternumber AS bigint) AS disasternumber
  FROM silver_hazard_cleaned.fema_disaster_declarations_clean
  WHERE disasternumber IS NOT NULL
),
stats AS (
  SELECT
    (SELECT count(*) FROM claims) AS claim_disaster_cnt,
    (SELECT count(*) FROM decls) AS decl_disaster_cnt,
    (SELECT count(*) FROM claims c JOIN decls d ON c.disasternumber = d.disasternumber) AS matched_disaster_cnt,
    (SELECT count(*) FROM claims c LEFT JOIN decls d ON c.disasternumber = d.disasternumber WHERE d.disasternumber IS NULL) AS unmatched_claim_disaster_cnt
)
SELECT
  *,
  CASE WHEN claim_disaster_cnt = 0 THEN 0.0
       ELSE 1.0 * matched_disaster_cnt / claim_disaster_cnt
  END AS match_rate
FROM stats
WHERE claim_disaster_cnt = 0
   OR (1.0 * matched_disaster_cnt / nullif(claim_disaster_cnt, 0)) < 0.90;
