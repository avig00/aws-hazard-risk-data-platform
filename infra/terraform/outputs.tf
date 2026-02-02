output "bronze_glue_database_name" {
  description = "Glue database name for bronze layer"
  value       = aws_glue_catalog_database.bronze.name
}

output "silver_glue_database_name" {
  description = "Glue database name for silver layer"
  value       = aws_glue_catalog_database.silver.name
}

output "glue_crawler_role_name" {
  value = aws_iam_role.glue_crawler_role.name
}
