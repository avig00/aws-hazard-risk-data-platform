#!/usr/bin/env bash
set -euo pipefail

# Dev validation workflow for the county_fips fixes.
#
# What this does:
# 1) creates dev Athena/Glue databases
# 2) runs the changed Silver jobs into dev Silver prefixes
# 3) creates temporary dev crawlers for the changed Silver outputs (+ unresolved outputs)
# 4) creates pass-through views in the dev Silver DB for unchanged Silver tables
# 5) runs the Gold Lambda against dev Silver + dev Gold
# 6) runs Athena validation queries against the dev marts
#
# Required env vars:
#   AWS_REGION
#   PLATFORM_S3_BUCKET
#   ATHENA_WORKGROUP
#   ATHENA_RESULTS_S3
#   BRONZE_PREFIX
#   CRAWLER_ROLE_ARN
#
# Optional env vars:
#   PROD_SILVER_DB       default: silver_hazard_cleaned
#   DEV_SILVER_DB        default: silver_hazard_cleaned_dev
#   DEV_GOLD_DB          default: gold_hazard_dev
#   DEV_SILVER_PREFIX    default: hazard/silver/dev
#   DEV_GOLD_PREFIX_ROOT default: hazard/gold/dev
#   RUN_DT               default: today in UTC

PROD_SILVER_DB="${PROD_SILVER_DB:-silver_hazard_cleaned}"
DEV_SILVER_DB="${DEV_SILVER_DB:-silver_hazard_cleaned_dev}"
DEV_GOLD_DB="${DEV_GOLD_DB:-gold_hazard_dev}"
DEV_SILVER_PREFIX="${DEV_SILVER_PREFIX:-hazard/silver/dev}"
DEV_GOLD_PREFIX_ROOT="${DEV_GOLD_PREFIX_ROOT:-hazard/gold/dev}"
RUN_DT="${RUN_DT:-$(date -u +%F)}"

required=(
  AWS_REGION
  PLATFORM_S3_BUCKET
  ATHENA_WORKGROUP
  ATHENA_RESULTS_S3
  BRONZE_PREFIX
  CRAWLER_ROLE_ARN
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required env var: $name" >&2
    exit 1
  fi
done

run_athena() {
  local database="$1"
  local sql="$2"
  local qid
  qid=$(aws athena start-query-execution \
    --region "$AWS_REGION" \
    --work-group "$ATHENA_WORKGROUP" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --query-string "$sql" \
    --query-execution-context "Database=${database}" \
    --query 'QueryExecutionId' \
    --output text)
  echo "$qid"
}

wait_athena() {
  local qid="$1"
  while true; do
    local state
    state=$(aws athena get-query-execution \
      --region "$AWS_REGION" \
      --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' \
      --output text)
    case "$state" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution \
          --region "$AWS_REGION" \
          --query-execution-id "$qid" \
          --output json >&2
        echo "Athena query failed: $qid" >&2
        return 1
        ;;
      *) sleep 3 ;;
    esac
  done
}

run_sql() {
  local database="$1"
  local sql="$2"
  local qid
  qid=$(run_athena "$database" "$sql")
  wait_athena "$qid"
  echo "$qid"
}

print_query_results() {
  local qid="$1"
  aws athena get-query-results \
    --region "$AWS_REGION" \
    --query-execution-id "$qid" \
    --output table
}

run_sql_and_print() {
  local database="$1"
  local sql="$2"
  local qid
  qid=$(run_sql "$database" "$sql")
  print_query_results "$qid"
}

start_glue_job_and_wait() {
  local job_name="$1"
  shift
  local run_id
  run_id=$(aws glue start-job-run \
    --region "$AWS_REGION" \
    --job-name "$job_name" \
    --arguments "$@" \
    --query 'JobRunId' \
    --output text)

  while true; do
    local state
    state=$(aws glue get-job-run \
      --region "$AWS_REGION" \
      --job-name "$job_name" \
      --run-id "$run_id" \
      --query 'JobRun.JobRunState' \
      --output text)
    case "$state" in
      SUCCEEDED) return 0 ;;
      FAILED|STOPPED|TIMEOUT|ERROR)
        aws glue get-job-run \
          --region "$AWS_REGION" \
          --job-name "$job_name" \
          --run-id "$run_id" \
          --output json
        echo "Glue job failed: $job_name ($run_id)" >&2
        return 1
        ;;
      *) sleep 10 ;;
    esac
  done
}

ensure_crawler_and_wait() {
  local crawler_name="$1"
  local target_path="$2"
  local db_name="$3"

  if ! aws glue get-crawler --region "$AWS_REGION" --name "$crawler_name" >/dev/null 2>&1; then
    aws glue create-crawler \
      --region "$AWS_REGION" \
      --name "$crawler_name" \
      --role "$CRAWLER_ROLE_ARN" \
      --database-name "$db_name" \
      --targets "{\"S3Targets\":[{\"Path\":\"$target_path\"}]}" \
      >/dev/null
  else
    aws glue update-crawler \
      --region "$AWS_REGION" \
      --name "$crawler_name" \
      --role "$CRAWLER_ROLE_ARN" \
      --database-name "$db_name" \
      --targets "{\"S3Targets\":[{\"Path\":\"$target_path\"}]}" \
      >/dev/null
  fi

  aws glue start-crawler --region "$AWS_REGION" --name "$crawler_name" >/dev/null || true

  while true; do
    local state
    state=$(aws glue get-crawler \
      --region "$AWS_REGION" \
      --name "$crawler_name" \
      --query 'Crawler.State' \
      --output text)
    [[ "$state" == "READY" ]] && return 0
    sleep 10
  done
}

invoke_lambda() {
  local fn_name="$1"
  local payload="$2"
  aws lambda invoke \
    --region "$AWS_REGION" \
    --function-name "$fn_name" \
    --payload "$payload" \
    --cli-binary-format raw-in-base64-out \
    /tmp/lambda_out.json >/dev/null
  cat /tmp/lambda_out.json
}

echo "==> Creating dev databases"
run_sql default "CREATE DATABASE IF NOT EXISTS ${DEV_SILVER_DB}"
run_sql default "CREATE DATABASE IF NOT EXISTS ${DEV_GOLD_DB}"

echo "==> Running changed Silver jobs into dev prefixes"
start_glue_job_and_wait "silver_noaa_details_clean" \
  "{\"--JOB_NAME\":\"silver_noaa_details_clean\",\"--S3_BUCKET\":\"${PLATFORM_S3_BUCKET}\",\"--BRONZE_PREFIX\":\"${BRONZE_PREFIX}\",\"--SILVER_PREFIX\":\"${DEV_SILVER_PREFIX}\"}"

start_glue_job_and_wait "silver_nri_counties_clean" \
  "{\"--JOB_NAME\":\"silver_nri_counties_clean\",\"--S3_BUCKET\":\"${PLATFORM_S3_BUCKET}\",\"--BRONZE_PREFIX\":\"${BRONZE_PREFIX}\",\"--SILVER_PREFIX\":\"${DEV_SILVER_PREFIX}\"}"

echo "==> Crawling changed Silver outputs into dev Silver DB"
ensure_crawler_and_wait "dev-silver-noaa-events-clean" \
  "s3://${PLATFORM_S3_BUCKET}/${DEV_SILVER_PREFIX}/noaa_events_clean/" \
  "${DEV_SILVER_DB}"

ensure_crawler_and_wait "dev-silver-noaa-events-clean-unresolved" \
  "s3://${PLATFORM_S3_BUCKET}/${DEV_SILVER_PREFIX}/noaa_events_clean_unresolved_county_fips/" \
  "${DEV_SILVER_DB}"

ensure_crawler_and_wait "dev-silver-noaa-events-clean-non-county" \
  "s3://${PLATFORM_S3_BUCKET}/${DEV_SILVER_PREFIX}/noaa_events_clean_non_county_zones/" \
  "${DEV_SILVER_DB}"

ensure_crawler_and_wait "dev-silver-nri-scores-clean" \
  "s3://${PLATFORM_S3_BUCKET}/${DEV_SILVER_PREFIX}/nri_scores_clean/" \
  "${DEV_SILVER_DB}"

ensure_crawler_and_wait "dev-silver-nri-scores-clean-unresolved" \
  "s3://${PLATFORM_S3_BUCKET}/${DEV_SILVER_PREFIX}/nri_scores_clean_unresolved_county_fips/" \
  "${DEV_SILVER_DB}"

echo "==> Creating pass-through views in dev Silver DB for unchanged Silver tables"
run_sql "${DEV_SILVER_DB}" "CREATE OR REPLACE VIEW ${DEV_SILVER_DB}.fema_disaster_declarations_clean AS SELECT * FROM ${PROD_SILVER_DB}.fema_disaster_declarations_clean"
run_sql "${DEV_SILVER_DB}" "CREATE OR REPLACE VIEW ${DEV_SILVER_DB}.fema_claims_clean AS SELECT * FROM ${PROD_SILVER_DB}.fema_claims_clean"
run_sql "${DEV_SILVER_DB}" "CREATE OR REPLACE VIEW ${DEV_SILVER_DB}.zip2county_xwalk_clean AS SELECT * FROM ${PROD_SILVER_DB}.zip2county_xwalk_clean"
run_sql "${DEV_SILVER_DB}" "CREATE OR REPLACE VIEW ${DEV_SILVER_DB}.census_clean AS SELECT * FROM ${PROD_SILVER_DB}.census_clean"

echo "==> Running Gold Lambda into dev Gold DB/prefix"
invoke_lambda "hazard-agent-gold" "{
  \"run_dt\": \"${RUN_DT}\",
  \"athena_db_silver\": \"${DEV_SILVER_DB}\",
  \"athena_db_gold\": \"${DEV_GOLD_DB}\",
  \"gold_prefix_root\": \"${DEV_GOLD_PREFIX_ROOT}\",
  \"marts\": [\"hazard_event_summary\", \"risk_feature_mart\"]
}"

echo "==> Running post-build quality suite against dev DBs"
invoke_lambda "hazard-agent-quality" "{
  \"run_dt\": \"${RUN_DT}\",
  \"athena_db_silver\": \"${DEV_SILVER_DB}\",
  \"athena_db_gold\": \"${DEV_GOLD_DB}\",
  \"quality\": {
    \"runs\": [
      {\"layer\": \"gold\", \"suite_name\": \"gold_marts\", \"sql_dir\": \"src/sql/validations/gold\", \"database\": \"${DEV_GOLD_DB}\"}
    ]
  }
}"

echo "==> Running spot-check Athena queries"
run_sql_and_print "${DEV_GOLD_DB}" "SELECT county_fips, COUNT(*) AS rows FROM ${DEV_GOLD_DB}.risk_feature_mart_current WHERE county_fips IN ('01340','00390') GROUP BY county_fips"
run_sql_and_print "${DEV_SILVER_DB}" "SELECT COUNT(*) AS unresolved_noaa_rows FROM ${DEV_SILVER_DB}.noaa_events_clean_unresolved_county_fips"
run_sql_and_print "${DEV_SILVER_DB}" "SELECT COUNT(*) AS non_county_noaa_rows FROM ${DEV_SILVER_DB}.noaa_events_clean_non_county_zones"
run_sql_and_print "${DEV_SILVER_DB}" "SELECT COUNT(*) AS unresolved_nri_rows FROM ${DEV_SILVER_DB}.nri_scores_clean_unresolved_county_fips"

echo "Dev validation workflow completed."
