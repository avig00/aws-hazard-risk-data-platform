variable "bucket_name" {
  type        = string
  description = "S3 bucket name for the project"
}

variable "glue_database_name" {
  type        = string
  description = "Glue database name for bronze layer"
  default     = "bronze_hazard"
}
