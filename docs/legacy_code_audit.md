# Legacy Code Audit

Purpose: identify what is part of the active production runtime versus what is legacy, manual, or debugging-only code that should not be treated as the primary execution path.

## Active Runtime Path

These are the supported production paths today:

- Silver transforms in [glue/jobs/silver](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/glue/jobs/silver)
- Glue asset deployment via [deploy_glue_assets.sh](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/glue/scripts/deploy_glue_assets.sh)
- Gold build orchestration via [gold_handler.py](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/src/lambda_handlers/gold_handler.py)
- Gold SQL templates in [src/sql/gold_ctas](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/src/sql/gold_ctas)
- Gold validation SQL in [src/sql/validations/gold](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/src/sql/validations/gold)
- Production crawlers defined in [crawlers.tf](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/infra/terraform/crawlers.tf)

## Legacy or Manual Paths (Retained)

These paths are not the primary production runtime, but they are still retained for manual investigation, comparison, or rollback reference:

- Legacy Gold SQL in [sql/gold](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/sql/gold)
- Legacy/manual Gold rebuild script in [run_phase4.sh](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/scripts/run_phase4.sh)
- Manual Bronze rebuild helper in [rebuild_bronze_non_noaa.sh](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/scripts/rebuild_bronze_non_noaa.sh)
- Manual DDL inspection helper in [show_bronze_ddls.sh](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/scripts/show_bronze_ddls.sh)
- IAM simulation helper in [simulate_iam.sh](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/scripts/simulate_iam.sh)
- Dev-only validation runner in [run_dev_validation.sh](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/scripts/run_dev_validation.sh)

These files should be treated as support tooling, not as the authoritative production execution path.

## Safe Cleanup Completed

Removed clearly generated or stale artifacts:

- checked-in Python bytecode under `glue/jobs/silver/__pycache__/`
- stale one-off quality output `scripts/silver__noaa_events_clean.json`

## Recommended Future Cleanup

1. Decide whether [sql/gold](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/sql/gold) should be archived into a clearly named `legacy/` folder or retained as a rollback reference.
2. Decide whether manual scripts in [scripts](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/scripts) should be split into:
   - supported operational scripts
   - ad hoc debugging scripts
3. Add a short README note that the CTAS path under `src/sql/gold_ctas` is the active Gold runtime and the older `sql/gold` path is legacy/manual.
