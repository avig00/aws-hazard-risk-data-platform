output "glue_database_name" {
  value = aws_glue_catalog_database.bronze.name
}

output "glue_crawler_role_name" {
  value = aws_iam_role.glue_crawler_role.name
}
