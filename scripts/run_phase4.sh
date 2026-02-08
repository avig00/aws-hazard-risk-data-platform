#!/usr/bin/env bash
set -euo pipefail

# Requirements:
# - aws cli configured
# - jq installed
#
# Set these:
WORKGROUP="${WORKGROUP:-athena-gold}"
DATABASE="${DATABASE:-gold_hazard}"
OUTPUT_S3="${OUTPUT_S3:-s3://aws-hazard-risk-vigamogh-dev/hazard/athena/results/}"

run_query () {
  local sql_file="$1"
  echo "Running: $sql_file"

  local qid
  qid=$(aws athena start-query-execution \
    --work-group "$WORKGROUP" \
    --query-execution-context Database="$DATABASE" \
    --result-configuration OutputLocation="$OUTPUT_S3" \
    --query-string "$(cat "$sql_file")" \
    | jq -r '.QueryExecutionId')

  [[ -n "${qid:-}" && "$qid" != "null" ]] || { echo "ERROR: Failed to start query for $sql_file"; exit 1; }

  while true; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" | jq -r '.QueryExecution.Status.State')
    if [[ "$state" == "SUCCEEDED" ]]; then
      break
    elif [[ "$state" == "FAILED" || "$state" == "CANCELLED" ]]; then
      echo "Query failed: $qid"
      aws athena get-query-execution --query-execution-id "$qid" | jq -r '.QueryExecution.Status.StateChangeReason'
      exit 1
    else
      sleep 2
    fi
  done
}

# Run a validation query and FAIL the script if it returns any rows.
run_validation_no_rows () {
  local sql_file="$1"
  echo "Validating (must return 0 rows): $sql_file"

  local qid
  qid=$(aws athena start-query-execution \
    --work-group "$WORKGROUP" \
    --query-execution-context Database="$DATABASE" \
    --result-configuration OutputLocation="$OUTPUT_S3" \
    --query-string "$(cat "$sql_file")" \
    | jq -r '.QueryExecutionId')

  [[ -n "${qid:-}" && "$qid" != "null" ]] || { echo "ERROR: Failed to start validation for $sql_file"; exit 1; }

  while true; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" | jq -r '.QueryExecution.Status.State')
    if [[ "$state" == "SUCCEEDED" ]]; then
      break
    elif [[ "$state" == "FAILED" || "$state" == "CANCELLED" ]]; then
      echo "Validation query failed: $qid"
      aws athena get-query-execution --query-execution-id "$qid" | jq -r '.QueryExecution.Status.StateChangeReason'
      exit 1
    else
      sleep 2
    fi
  done

  # Fetch results and ensure there are NO data rows (only header row is allowed).
  # get-query-results RowCount includes the header row, so PASS => RowCount == 1
  local row_count
  row_count=$(aws athena get-query-results --query-execution-id "$qid" --max-results 1 | jq -r '.ResultSet.Rows | length')

  # If Athena returns >1 rows in the first page, the validation produced data rows => FAIL.
  if [[ "${row_count:-0}" -gt 1 ]]; then
    echo "VALIDATION FAILED (returned rows). QueryExecutionId: $qid"
    echo "Sample failing rows (first page):"
    aws athena get-query-results --query-execution-id "$qid" --max-results 20 \
      | jq -r '.ResultSet.Rows[] | [.Data[].VarCharValue] | @tsv' || true
    exit 1
  fi

  echo "PASS: $sql_file"
}

cleanup_prefix () {
  local s3_prefix="$1"
  echo "Cleaning S3 prefix: $s3_prefix"
  aws s3 rm --recursive "$s3_prefix" >/dev/null 2>&1 || true
}

# ---- Preflight checks (fast fail) ----
aws s3 ls s3://aws-hazard-risk-vigamogh-dev/hazard/gold/_seeds/hazard_type_map/ | grep -q 'hazard_type_map\.csv' \
  || { echo "ERROR: hazard_type_map.csv missing in S3: s3://aws-hazard-risk-vigamogh-dev/hazard/gold/_seeds/hazard_type_map/"; exit 1; }

# ---- Phase 4 execution order ----
run_query sql/gold/01a_drop_hazard_type_map.sql
run_query sql/gold/01b_hazard_type_map.sql

run_query sql/gold/02a_drop_county_dim.sql
cleanup_prefix s3://aws-hazard-risk-vigamogh-dev/hazard/gold/county_dim/
run_query sql/gold/02b_county_dim.sql

run_query sql/gold/03_define_years_in_scope.sql

run_query sql/gold/04a_drop_county_year_universe.sql
cleanup_prefix s3://aws-hazard-risk-vigamogh-dev/hazard/gold/county_year_universe/
run_query sql/gold/04b_county_year_universe.sql

run_query sql/gold/05a_drop_hazard_event_summary.sql
cleanup_prefix s3://aws-hazard-risk-vigamogh-dev/hazard/gold/hazard_event_summary/
run_query sql/gold/05b_hazard_event_summary.sql

run_query sql/gold/views/06a_noaa_county_year.sql
run_query sql/gold/views/06b_noaa_county_year_by_group.sql

run_query sql/gold/views/07a_nri_county.sql
run_query sql/gold/views/07b_census_county_latest.sql

run_query sql/gold/views/08a_fema_decls_county_year.sql
run_query sql/gold/views/08b_fema_claims_by_disaster.sql
run_query sql/gold/views/08c_fema_disaster_county_map.sql
run_query sql/gold/views/08d_fema_disaster_county_counts.sql
run_query sql/gold/views/08e_fema_claims_county_year.sql

run_query sql/gold/09a_drop_risk_feature_mart.sql
cleanup_prefix s3://aws-hazard-risk-vigamogh-dev/hazard/gold/risk_feature_mart/
run_query sql/gold/09b_risk_feature_mart.sql

run_query sql/gold/10a_drop_county_risk_scores.sql
run_query sql/gold/10b_county_risk_scores.sql

echo "Phase 4 build complete. Now running validations..."

# ---- Validations ----
run_validation_no_rows sql/quality/gold/01_hazard_type_map_unmapped.sql
run_validation_no_rows sql/quality/gold/02_county_dim_uniqueness.sql
run_validation_no_rows sql/quality/gold/03_universe_expected_rowcount.sql
run_validation_no_rows sql/quality/gold/04_universe_uniqueness.sql
run_validation_no_rows sql/quality/gold/05_hazard_event_summary_uniqueness.sql
run_validation_no_rows sql/quality/gold/06_hazard_event_summary_nonnegative.sql
run_validation_no_rows sql/quality/gold/07_risk_feature_mart_rowcount_equals_universe.sql
run_validation_no_rows sql/quality/gold/08_risk_feature_mart_uniqueness.sql
run_validation_no_rows sql/quality/gold/09_risk_feature_mart_nonnegative.sql
run_validation_no_rows sql/quality/gold/10_risk_feature_mart_coverage.sql
run_validation_no_rows sql/quality/gold/11_fema_claims_county_year_nonnegative.sql
run_validation_no_rows sql/quality/gold/12_fema_claims_join_coverage.sql
run_validation_no_rows sql/quality/gold/13_nri_parse_sanity.sql

echo "All Phase 4 queries executed and validations passed."
