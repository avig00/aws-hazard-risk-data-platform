-- Template var: {{run_ds_nodash}}
-- Template var: {{athena_db_gold}}

CREATE OR REPLACE VIEW {{athena_db_gold}}.risk_feature_mart_current AS
SELECT *
FROM {{athena_db_gold}}.risk_feature_mart__{{run_ds_nodash}};
