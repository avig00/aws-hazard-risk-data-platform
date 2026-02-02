#!/usr/bin/env bash
set -euo pipefail

# deploy_glue_assets.sh
#
# Purpose:
#   Upload Phase 3 Glue scripts + shared library to S3 so Glue Jobs can run them.
#
# Usage:
#   ./glue/scripts/deploy_glue_assets.sh aws-hazard-risk-vigamogh-dev hazard/glue-assets
#
# Outputs uploaded to:
#   s3://<bucket>/<assets_prefix>/lib/*.py
#   s3://<bucket>/<assets_prefix>/jobs/silver/*.py

BUCKET="${1:-aws-hazard-risk-vigamogh-dev}"
ASSETS_PREFIX="${2:-hazard/glue-assets}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Uploading Glue assets to s3://${BUCKET}/${ASSETS_PREFIX}/ ..."

# Future-proof: upload ALL helper modules in glue/lib/
aws s3 sync "${ROOT_DIR}/glue/lib/" \
  "s3://${BUCKET}/${ASSETS_PREFIX}/lib/" \
  --exclude "*" --include "*.py"

# Upload ALL silver job scripts
aws s3 cp "${ROOT_DIR}/glue/jobs/silver/" \
  "s3://${BUCKET}/${ASSETS_PREFIX}/jobs/silver/" \
  --recursive --exclude "*" --include "*.py"

echo "Done."
echo "Lib : s3://${BUCKET}/${ASSETS_PREFIX}/lib/"
echo "Jobs: s3://${BUCKET}/${ASSETS_PREFIX}/jobs/silver/"
