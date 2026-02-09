-- Template var: {{run_ds_nodash}}

CREATE OR REPLACE VIEW risk_feature_mart_current AS
SELECT * FROM risk_feature_mart__{{run_ds_nodash}};
