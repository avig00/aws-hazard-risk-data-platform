#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Defaults (override via env if needed)
# ----------------------------
ROLE_NAME_DEFAULT="hazard-risk-agent-lambda-role"
REGION_DEFAULT="us-east-1"
BUCKET_DEFAULT="aws-hazard-risk-vigamogh-dev"
GOLD_DB_DEFAULT="gold_hazard"
WORKGROUP_DEFAULT="athena-gold"

ROLE_NAME="${ROLE_NAME:-$ROLE_NAME_DEFAULT}"
REGION="${REGION:-$REGION_DEFAULT}"
BUCKET="${BUCKET:-$BUCKET_DEFAULT}"
GOLD_DB="${GOLD_DB:-$GOLD_DB_DEFAULT}"
WORKGROUP="${WORKGROUP:-$WORKGROUP_DEFAULT}"

ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}}"

need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 2; }; }
need_cmd aws
need_cmd jq

echo "IAM Simulation Configuration"
echo "ACCOUNT_ID : $ACCOUNT_ID"
echo "REGION     : $REGION"
echo "ROLE_NAME  : $ROLE_NAME"
echo "ROLE_ARN   : $ROLE_ARN"
echo "BUCKET     : $BUCKET"
echo "GOLD_DB    : $GOLD_DB"
echo "WORKGROUP  : $WORKGROUP"
echo ""

# ----------------------------
# Quick sanity: show role boundary (if any) + attached inline policy doc
# ----------------------------
echo "Role sanity check"
aws iam get-role --role-name "$ROLE_NAME" --output json \
| jq -r '{
    RoleName: .Role.RoleName,
    Arn: .Role.Arn,
    PermissionsBoundary: (.Role.PermissionsBoundary.Arn // null)
  }'
echo ""

# ----------------------------
# Resource ARNs
# ----------------------------
GLUE_CATALOG_RESOURCES=(
  "arn:aws:glue:${REGION}:${ACCOUNT_ID}:catalog"
  "arn:aws:glue:${REGION}:${ACCOUNT_ID}:database/${GOLD_DB}"
  "arn:aws:glue:${REGION}:${ACCOUNT_ID}:table/${GOLD_DB}/*"
)

ATHENA_WORKGROUP_RESOURCES=(
  "arn:aws:athena:${REGION}:${ACCOUNT_ID}:workgroup/${WORKGROUP}"
)

S3_BUCKET_RESOURCE="arn:aws:s3:::${BUCKET}"
S3_OBJECT_RESOURCES=(
  "arn:aws:s3:::${BUCKET}/athena-results/*"
  "arn:aws:s3:::${BUCKET}/hazard/athena/results/*"
  "arn:aws:s3:::${BUCKET}/hazard/ops*"
)

# Discover Glue job + crawler ARNs (best-effort)
echo "Discovering Glue job + crawler ARNs (best-effort)"
GLUE_JOB_ARNS=()
while read -r name; do
  [[ -z "$name" ]] && continue
  GLUE_JOB_ARNS+=("arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/${name}")
done < <(aws glue get-jobs --max-results 200 --output json | jq -r '.Jobs[].Name' 2>/dev/null || true)

GLUE_CRAWLER_ARNS=()
while read -r name; do
  [[ -z "$name" ]] && continue
  GLUE_CRAWLER_ARNS+=("arn:aws:glue:${REGION}:${ACCOUNT_ID}:crawler/${name}")
done < <(aws glue get-crawlers --max-results 200 --output json | jq -r '.Crawlers[].Name' 2>/dev/null || true)

echo "Found Glue jobs    : ${#GLUE_JOB_ARNS[@]}"
echo "Found Glue crawlers: ${#GLUE_CRAWLER_ARNS[@]}"
echo ""

# ----------------------------
# Helper to run a simulation and print only denies
# ----------------------------
simulate_denies() {
  local title="$1"; shift
  local -a actions=(); local -a resources=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --actions) shift; while [[ $# -gt 0 && "$1" != --resources ]]; do actions+=("$1"); shift; done ;;
      --resources) shift; while [[ $# -gt 0 ]]; do resources+=("$1"); shift; done ;;
      *) echo "Internal error parsing args"; exit 2 ;;
    esac
  done

  echo "$title"
  aws iam simulate-principal-policy \
    --policy-source-arn "$ROLE_ARN" \
    --action-names "${actions[@]}" \
    --resource-arns "${resources[@]}" \
    --output json \
  | jq -r '
      .EvaluationResults[]
      | select(.EvalDecision != "allowed")
      | "\(.EvalActionName)\t\(.EvalResourceName)\t\(.EvalDecision)"
    ' | sort -u
  echo ""
}

DENIES_TMP="$(mktemp)"
touch "$DENIES_TMP"

# ----------------------------
# 1) Glue Catalog (tables/partitions/views metadata)
# ----------------------------
{
  simulate_denies "Denies: Glue catalog/table/partition operations" \
    --actions \
      glue:GetDatabase glue:GetDatabases \
      glue:GetTable glue:GetTables \
      glue:GetPartition glue:GetPartitions \
      glue:CreateTable glue:UpdateTable glue:DeleteTable \
    --resources \
      "${GLUE_CATALOG_RESOURCES[@]}"
} | tee -a "$DENIES_TMP"

# ----------------------------
# 2) Athena (workgroup-scoped check)
# ----------------------------
{
  simulate_denies "Denies: Athena workgroup operations" \
    --actions \
      athena:StartQueryExecution athena:GetQueryExecution athena:GetQueryResults athena:GetWorkGroup \
    --resources \
      "${ATHENA_WORKGROUP_RESOURCES[@]}"
} | tee -a "$DENIES_TMP"

# ----------------------------
# 3) S3 bucket-level actions
# ----------------------------
{
  simulate_denies "Denies: S3 bucket-level operations" \
    --actions \
      s3:ListBucket s3:GetBucketLocation s3:GetBucketAcl \
    --resources \
      "$S3_BUCKET_RESOURCE"
} | tee -a "$DENIES_TMP"

# ----------------------------
# 4) S3 object-level actions
# ----------------------------
{
  simulate_denies "Denies: S3 object-level operations" \
    --actions \
      s3:GetObject s3:PutObject s3:DeleteObject s3:AbortMultipartUpload s3:ListBucketMultipartUploads \
    --resources \
      "${S3_OBJECT_RESOURCES[@]}"
} | tee -a "$DENIES_TMP"

# ----------------------------
# 5) Glue Jobs (only if we discovered any)
# ----------------------------
if [[ "${#GLUE_JOB_ARNS[@]}" -gt 0 ]]; then
  {
    simulate_denies "Denies: Glue job operations (discovered jobs)" \
      --actions \
        glue:StartJobRun glue:GetJobRun glue:GetJobRuns \
      --resources \
        "${GLUE_JOB_ARNS[@]}"
  } | tee -a "$DENIES_TMP"
else
  echo "Skipping Glue job simulation (no jobs discovered)."
  echo ""
fi

# ----------------------------
# 6) Glue Crawlers (only if discovered any)
# ----------------------------
if [[ "${#GLUE_CRAWLER_ARNS[@]}" -gt 0 ]]; then
  {
    simulate_denies "Denies: Glue crawler operations (discovered crawlers)" \
      --actions \
        glue:StartCrawler glue:GetCrawler \
      --resources \
        "${GLUE_CRAWLER_ARNS[@]}"
  } | tee -a "$DENIES_TMP"
else
  echo "Skipping Glue crawler simulation (no crawlers discovered)."
  echo ""
fi

# ----------------------------
# Final: count denies
# ----------------------------
# Keep only actual deny lines (tab-separated) from the combined output
DENY_COUNT="$(grep -E $'^[a-z0-9]+:[A-Za-z0-9]+\t' "$DENIES_TMP" | wc -l | tr -d ' ')"

if [[ "$DENY_COUNT" -eq 0 ]]; then
  echo "No denies found across simulations."
  rm -f "$DENIES_TMP"
  exit 0
else
  echo "Found $DENY_COUNT deny lines across simulations."
  echo "Raw output file: $DENIES_TMP"
  exit 1
fi
