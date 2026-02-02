# variables.tf
#
# Purpose:
#   Central Terraform variables for the Hazard Risk data platform.
#   This file defines:
#     - S3 bucket + path prefixes (bronze/silver/assets/temp)
#     - Glue databases (bronze/silver)
#     - Glue job role + runtime sizing
#     - Glue workflow naming
#     - S3 lifecycle cleanup (Athena query results, Glue temp)

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

# -----------------------
# Glue Workflow
# -----------------------
variable "glue_workflow_name" {
  type        = string
  description = "Glue workflow name for Phase 3 Silver"
  default     = "silver-workflow"
}

# -----------------------
# S3 Lifecycle Cleanup
# -----------------------

# This should match your Athena Workgroup output location prefix.
# Common default is "athena-results/" but verify your workgroup setting.
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
