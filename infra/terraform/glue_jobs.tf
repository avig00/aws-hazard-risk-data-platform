# glue_jobs.tf
#
# Purpose:
#   Phase 3 Glue Spark Jobs (Bronze -> Silver)
#
# Scripts live in S3:
#   s3://<bucket>/<assets_prefix>/jobs/silver/*.py
# Shared lib:
#   s3://<bucket>/<assets_prefix>/lib/silver_utils.py

locals {
  assets_base = "s3://${var.bucket_name}/${var.assets_prefix}"
  temp_dir    = "s3://${var.bucket_name}/${var.glue_temp_prefix}/"

  extra_py_files = "${local.assets_base}/lib/silver_utils.py"

  common_args = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = local.temp_dir

    "--S3_BUCKET"     = var.bucket_name
    "--BRONZE_PREFIX" = var.bronze_prefix
    "--SILVER_PREFIX" = var.silver_prefix

    "--extra-py-files" = local.extra_py_files
  }
}

resource "aws_glue_job" "silver_noaa_details_clean" {
  name     = "silver_noaa_details_clean"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers_noaa
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.assets_base}/jobs/silver/01_noaa_details_clean.py"
  }

  default_arguments = local.common_args
}

resource "aws_glue_job" "silver_fema_disaster_declarations_clean" {
  name     = "silver_fema_disaster_declarations_clean"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers_default
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.assets_base}/jobs/silver/02_fema_disaster_declarations_clean.py"
  }

  default_arguments = local.common_args
}

resource "aws_glue_job" "silver_fema_claims_clean" {
  name     = "silver_fema_claims_clean"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers_default
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.assets_base}/jobs/silver/03_fema_claims_clean.py"
  }

  default_arguments = local.common_args
}

resource "aws_glue_job" "silver_nri_counties_clean" {
  name     = "silver_nri_counties_clean"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers_default
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.assets_base}/jobs/silver/04_nri_counties_clean.py"
  }

  default_arguments = local.common_args
}

resource "aws_glue_job" "silver_census_clean" {
  name     = "silver_census_clean"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers_default
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.assets_base}/jobs/silver/05_census_clean.py"
  }

  default_arguments = local.common_args
}
