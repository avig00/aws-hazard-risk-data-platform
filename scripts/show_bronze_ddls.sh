#!/usr/bin/env bash
set -euo pipefail

# ========= CONFIG =========
WORKGROUP="primary"   # change if needed
DB="bronze_hazard_raw"
OUT="s3://aws-hazard-risk-vigamogh-dev/athena-results/"
# ===========================

runq() {
  local SQL="$1"
  local QID STATE

  QID=$(aws athena start-query-execution \
    --work-group "$WORKGROUP" \
    --query-execution-context Database="$DB" \
    --query-string "$SQL" \
    --result-configuration OutputLocation="$OUT" \
    --output text \
    --query 'QueryExecutionId')

  echo "QID=$QID"

  while true; do
    STATE=$(aws athena get-query-execution \
      --query-execution-id "$QID" \
      --output text \
      --query 'QueryExecution.Status.State')

    [[ "$STATE" == "SUCCEEDED" || "$STATE" == "FAILED" || "$STATE" == "CANCELLED" ]] && break
    sleep 2
  done

  echo "STATE=$STATE"
  echo "$QID"
}

qres() {
  local QID="$1"
  aws athena get-query-results \
    --query-execution-id "$QID" \
    --output json \
    | jq -r '.ResultSet.Rows[].Data[0].VarCharValue' \
    | sed 1d
}

show_ddl() {
  local TABLE="$1"
  echo "=============================================================="
  echo "SHOW CREATE TABLE ${DB}.${TABLE};"
  echo "=============================================================="

  QID=$(runq "SHOW CREATE TABLE ${DB}.${TABLE};" | tail -n 1)
  qres "$QID"
  echo
}

# ============================================================
# TABLES TO INSPECT
# ============================================================

TABLES=(
  disaster_declarations_csv
  housing_assistance_owners_csv
  housing_assistance_renters_csv
  nri_counties_csv
  acs5_2022_b01001_county_csv
  acs5_2022_b15003_county_csv
  acs5_2022_b23025_county_csv
  acs5_2022_b19013_county_csv
  acs5_2022_b25077_county_csv
)

for t in "${TABLES[@]}"; do
  show_ddl "$t"
done

echo "Done."
