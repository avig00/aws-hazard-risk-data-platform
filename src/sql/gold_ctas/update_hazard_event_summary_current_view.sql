-- Template var: {{run_ds_nodash}}
-- Template var: {{athena_db_gold}}

CREATE OR REPLACE VIEW {{athena_db_gold}}.hazard_event_summary_current AS
SELECT *
FROM {{athena_db_gold}}.hazard_event_summary__{{run_ds_nodash}};
