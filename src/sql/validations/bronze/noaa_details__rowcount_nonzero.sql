SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failures
FROM bronze_hazard_raw.details;