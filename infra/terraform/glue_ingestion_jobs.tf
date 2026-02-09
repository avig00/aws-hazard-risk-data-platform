locals {
  code_base        = "s3://${var.bucket_name}/hazard/code"
  packages_zip     = "${local.code_base}/packages/src.zip"
  glue_jobs_prefix = "${local.code_base}/glue_jobs"

  bronze_prefix     = "hazard/bronze"
  ops_prefix        = "hazard/ops"
  athena_results_s3 = "s3://${var.bucket_name}/hazard/athena/results/"
}

# -----------------------
# IAM Role for Glue Ingestion Jobs
# -----------------------
data "aws_iam_policy_document" "glue_ingestion_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_ingestion_role" {
  name               = "glue-ingestion-role"
  assume_role_policy = data.aws_iam_policy_document.glue_ingestion_assume.json
}

data "aws_iam_policy_document" "glue_ingestion_policy" {
  # logs
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }

  # read code artifacts
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::${var.bucket_name}",
      "arn:aws:s3:::${var.bucket_name}/hazard/code/*"
    ]
  }

  # write Bronze + Ops
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "arn:aws:s3:::${var.bucket_name}/${local.bronze_prefix}/*",
      "arn:aws:s3:::${var.bucket_name}/${local.ops_prefix}/*"
    ]
  }
}

resource "aws_iam_policy" "glue_ingestion_policy" {
  name   = "glue-ingestion-policy"
  policy = data.aws_iam_policy_document.glue_ingestion_policy.json
}

resource "aws_iam_role_policy_attachment" "glue_ingestion_attach" {
  role       = aws_iam_role.glue_ingestion_role.name
  policy_arn = aws_iam_policy.glue_ingestion_policy.arn
}

# baseline Glue policy
resource "aws_iam_role_policy_attachment" "glue_service_attach" {
  role       = aws_iam_role.glue_ingestion_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# -----------------------
# Glue Jobs (PythonShell)
# -----------------------

resource "aws_glue_job" "bronze_ingest_noaa" {
  name     = "bronze_ingest_noaa"
  role_arn = aws_iam_role.glue_ingestion_role.arn

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.glue_jobs_prefix}/bronze_ingest_noaa.py"
  }

  default_arguments = {
    "--extra-py-files"            = local.packages_zip
    "--additional-python-modules" = "requests==2.32.3"
    "--TempDir"                   = local.athena_results_s3

    "--PLATFORM_S3_BUCKET" = var.bucket_name
    "--BRONZE_PREFIX"      = local.bronze_prefix
    "--OPS_PREFIX"         = local.ops_prefix

    "--NOAA_INGEST_MODE"         = "monthly"
    "--NOAA_BACKFILL_START_YEAR" = "2000"
    "--NOAA_BACKFILL_END_YEAR"   = "2023"
    "--NOAA_MONTHLY_YEAR_OFFSET" = "0"
  }

  max_capacity = 1.0
  timeout      = 25
  glue_version = "3.0"
}

resource "aws_glue_job" "bronze_ingest_fema" {
  name     = "bronze_ingest_fema"
  role_arn = aws_iam_role.glue_ingestion_role.arn

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.glue_jobs_prefix}/bronze_ingest_fema.py"
  }

  default_arguments = {
    "--extra-py-files"            = local.packages_zip
    "--additional-python-modules" = "requests==2.32.3"
    "--TempDir"                   = local.athena_results_s3

    "--PLATFORM_S3_BUCKET" = var.bucket_name
    "--BRONZE_PREFIX"      = local.bronze_prefix
    "--OPS_PREFIX"         = local.ops_prefix
  }

  max_capacity = 1.0
  timeout      = 25
  glue_version = "3.0"
}

resource "aws_glue_job" "bronze_ingest_nri" {
  name     = "bronze_ingest_nri"
  role_arn = aws_iam_role.glue_ingestion_role.arn

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.glue_jobs_prefix}/bronze_ingest_nri.py"
  }

  default_arguments = {
    "--extra-py-files"            = local.packages_zip
    "--additional-python-modules" = "requests==2.32.3"
    "--TempDir"                   = local.athena_results_s3

    "--PLATFORM_S3_BUCKET" = var.bucket_name
    "--BRONZE_PREFIX"      = local.bronze_prefix
    "--OPS_PREFIX"         = local.ops_prefix
  }

  max_capacity = 1.0
  timeout      = 35
  glue_version = "3.0"
}

resource "aws_glue_job" "bronze_ingest_census" {
  name     = "bronze_ingest_census"
  role_arn = aws_iam_role.glue_ingestion_role.arn

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.glue_jobs_prefix}/bronze_ingest_census.py"
  }

  default_arguments = {
    "--extra-py-files"            = local.packages_zip
    "--additional-python-modules" = "requests==2.32.3"
    "--TempDir"                   = local.athena_results_s3

    "--PLATFORM_S3_BUCKET" = var.bucket_name
    "--BRONZE_PREFIX"      = local.bronze_prefix
    "--OPS_PREFIX"         = local.ops_prefix
  }

  max_capacity = 1.0
  timeout      = 25
  glue_version = "3.0"
}
