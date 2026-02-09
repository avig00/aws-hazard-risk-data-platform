# variables.tf
#
# Purpose:
#   Central Terraform variables for the Hazard Risk data platform.
#   This file defines:
#     - S3 bucket + path prefixes (bronze/silver/gold/assets/temp/ops)
#     - Glue databases (bronze/silver/gold)
#     - Glue job role + runtime sizing
#     - Athena results lifecycle cleanup (query outputs)
#     - Glue temp lifecycle cleanup
#
# Notes:
#   - MWAA has been removed, so no workflow/airflow variables remain.
#   - Step Functions/Lambda orchestration reads bucket + ops prefix (and workgroup elsewhere if you define it).

variable "bucket_name" {
  type        = string
  description = "S3 bucket name for the project"
}

# -----------------------
# Glue Databases
# -----------------------
variable "glue_database_name" {
  type        = string
  description = "Glue database name for bronze layer"
  default     = "bronze_hazard_raw"
}

variable "silver_glue_database_name" {
  type        = string
  description = "Glue database name for silver layer"
  default     = "silver_hazard_cleaned"
}

variable "gold_glue_database_name" {
  type        = string
  description = "Glue database name for gold layer"
  default     = "gold_hazard"
}

# -----------------------
# Glue Jobs
# -----------------------
variable "glue_bronze_jobs" {
  type        = list(string)
  description = "Glue job names for Bronze ingestion (PythonShell jobs)"
  default     = []
}

variable "glue_silver_jobs" {
  type        = list(string)
  description = "Glue job names for Silver transforms (Spark jobs)"
  default     = []
}

# -----------------------
# S3 Prefixes (folder roots)
# -----------------------
variable "bronze_prefix" {
  type        = string
  description = "S3 prefix for bronze layer root"
  default     = "hazard/bronze"
}

variable "silver_prefix" {
  type        = string
  description = "S3 prefix for silver layer root"
  default     = "hazard/silver"
}

variable "gold_prefix" {
  type        = string
  description = "S3 prefix for gold layer root"
  default     = "hazard/gold"
}

variable "assets_prefix" {
  type        = string
  description = "S3 prefix where Glue job scripts + libs are uploaded"
  default     = "hazard/glue-assets"
}

# IMPORTANT: end with "/" for lifecycle prefix matching
variable "glue_temp_prefix" {
  type        = string
  description = "S3 prefix for Glue TempDir (Spark spills, bookmarks, etc.)"
  default     = "hazard/glue-tmp/"
}

# Ops artifacts written by Step Functions/Lambda agents (summaries, QA reports, run metadata)
variable "ops_prefix" {
  type        = string
  description = "S3 prefix where orchestration ops artifacts are written (agent summaries, validation reports)"
  default     = "hazard/ops"
}

# -----------------------
# IAM Role names
# -----------------------
variable "glue_job_role_name" {
  type        = string
  description = "IAM role name for Glue Spark jobs (Silver ETL)"
  default     = "glue-job-hazard-silver"
}

# -----------------------
# Glue Job Runtime Settings
# -----------------------
variable "glue_version" {
  type        = string
  description = "Glue version for Spark jobs"
  default     = "4.0"
}

variable "glue_worker_type" {
  type        = string
  description = "Glue worker type (e.g., G.1X, G.2X)"
  default     = "G.1X"
}

variable "glue_workers_default" {
  type        = number
  description = "Default number of workers for most Silver jobs"
  default     = 2
}

variable "glue_workers_noaa" {
  type        = number
  description = "Workers for NOAA details clean job (largest dataset)"
  default     = 5
}

variable "glue_timeout_minutes" {
  type        = number
  description = "Timeout for Glue jobs in minutes"
  default     = 60
}

# ---------------------------
# Athena Workbook and Results
# ---------------------------
variable "athena_workgroup" {
  type        = string
  description = "Athena workgroup name used for Gold queries/validations"
  default     = "athena-gold"
}

variable "athena_results_s3" {
  type        = string
  description = "Full S3 URI where Athena query results are written"
}


# -----------------------
# S3 Lifecycle Cleanup
# -----------------------
variable "athena_results_prefix" {
  type        = string
  description = "S3 prefix where Athena query results are written"
  default     = "athena-results/"
}

variable "athena_results_prefix_hazard" {
  type        = string
  description = "Alternate/legacy Athena results prefix to expire (e.g., hazard/athena/results/)"
  default     = "hazard/athena/results/"
}

variable "athena_results_expire_days" {
  type        = number
  description = "Days after which Athena query result objects expire"
  default     = 14
}

variable "glue_temp_expire_days" {
  type        = number
  description = "Days after which Glue temp objects expire"
  default     = 7
}

variable "abort_multipart_upload_days" {
  type        = number
  description = "Abort incomplete multipart uploads after N days (S3 hygiene)"
  default     = 7
}
