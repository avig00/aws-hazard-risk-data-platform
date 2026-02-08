# glue.tf
#
# Purpose:
#   Glue Data Catalog databases for Bronze + Silver + Gold layers.
#   (IAM roles live in iam*.tf)

resource "aws_glue_catalog_database" "bronze" {
  name = var.glue_database_name
}

resource "aws_glue_catalog_database" "silver" {
  name = var.silver_glue_database_name
}

resource "aws_glue_catalog_database" "gold" {
  name = var.gold_glue_database_name
}
