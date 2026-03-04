# NRI Null Audit

Purpose: audit the remaining rows in `risk_feature_mart_current` where `nri_risk_score` is `NULL` after the county-FIPS fixes.

## Scope

This audit is for the residual `NULL` NRI rows that remain after removing invalid county keys such as `01340` and `00390`.

These rows should now be treated as one of three categories:

1. Acceptable residual edge cases:
- county exists in the mart, but no canonical NRI row exists for that county in Silver
- county is a legitimate special-case geography not covered by the current NRI source

2. Acceptable source-value gaps:
- county exists in `silver_hazard_cleaned.nri_scores_clean`
- the source row itself has `risk_score IS NULL`
- this is currently expected for Puerto Rico and certain territories in the NRI source

3. Unexpected gaps:
- county exists in `silver_hazard_cleaned.nri_scores_clean`
- the source row has a non-null `risk_score`
- but the final mart still shows `nri_risk_score IS NULL`
- this would indicate a join or transformation defect and should be investigated immediately

## Audit Queries

Use the SQL in:

- [risk_feature_mart__nri_null_summary.sql](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/src/sql/audits/gold/risk_feature_mart__nri_null_summary.sql)
- [risk_feature_mart__nri_null_counties.sql](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/src/sql/audits/gold/risk_feature_mart__nri_null_counties.sql)
- [risk_feature_mart__nri_null_detail_sample.sql](/Users/vikasvig/Desktop/portfolio_projects/aws-hazard-risk-data-platform/src/sql/audits/gold/risk_feature_mart__nri_null_detail_sample.sql)

Template vars:

- `{{athena_db_gold}}`
- `{{athena_db_silver}}`

For production, use:

- `{{athena_db_gold}} = gold_hazard`
- `{{athena_db_silver}} = silver_hazard_cleaned`

## Recommended Review Order

1. Run the summary query first.
- Confirm most residual rows fall into `missing_in_nri_reference` or `present_in_nri_but_score_null`.
- If any rows fall into `unexpected_null_after_join`, treat that as a possible regression.

2. Run the county ranking query next.
- Focus on counties with the most null-bearing year rows or the highest NOAA/FEMA activity.
- Those are the best candidates for any final mapping follow-up.

3. Run the detail sample query last.
- Use this to manually confirm whether the remaining rows are acceptable edge cases.

## Exit Criteria

The project can be considered closed on NRI null handling when:

- no invalid county FIPS remain in `risk_feature_mart_current`
- no material cluster of `unexpected_null_after_join` rows is found
- the remaining `NULL` NRI rows are documented as acceptable residual source-coverage gaps or intentionally deferred mapping edge cases

## Current Production Interpretation

Based on the final production audit:

- most residual `NULL` NRI rows are `present_in_nri_but_score_null`
- Puerto Rico (`72`) is the dominant driver
- American Samoa (`60`), Guam (`66`), and the Northern Mariana Islands (`69`) follow the same pattern
- for these territories, `risk_score`, `sovi_score`, and `resl_score` are null in the current NRI source, while `eal_score` remains populated

These rows should be treated as source-data limitations, not county-key or join defects.
