-- Template var: {{run_ds_nodash}}

CREATE OR REPLACE VIEW hazard_event_summary_current AS
SELECT * FROM hazard_event_summary__{{run_ds_nodash}};
