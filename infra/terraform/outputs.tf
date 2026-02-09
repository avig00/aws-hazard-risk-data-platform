output "bronze_glue_database_name" {
  description = "Glue database name for bronze layer"
  value       = aws_glue_catalog_database.bronze.name
}

output "silver_glue_database_name" {
  description = "Glue database name for silver layer"
  value       = aws_glue_catalog_database.silver.name
}

output "gold_glue_database_name" {
  description = "Glue database name for gold layer"
  value       = aws_glue_catalog_database.gold.name
}

output "glue_crawler_role_name" {
  value = aws_iam_role.glue_crawler_role.name
}

output "athena_gold_workgroup_name" {
  value = aws_athena_workgroup.gold.name
}

output "athena_gold_execution_role" {
  value = aws_iam_role.athena_gold_role.name
}

output "bucket_name" {
  value = var.bucket_name
}


