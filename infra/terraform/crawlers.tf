locals {
  base = "s3://${var.bucket_name}/hazard/bronze"
}

# NOAA (3)
resource "aws_glue_crawler" "noaa_details" {
  name          = "bronze-noaa-details"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/noaa/details/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "noaa_fatalities" {
  name          = "bronze-noaa-fatalities"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/noaa/fatalities/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "noaa_locations" {
  name          = "bronze-noaa-locations"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/noaa/locations/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

# FEMA (3)
resource "aws_glue_crawler" "fema_disaster_declarations" {
  name          = "bronze-fema-disaster-declarations"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/fema/disaster_declarations/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "fema_ha_owners" {
  name          = "bronze-fema-housing-assistance-owners"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/fema/housing_assistance_owners/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "fema_ha_renters" {
  name          = "bronze-fema-housing-assistance-renters"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/fema/housing_assistance_renters/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

# NRI (1)
resource "aws_glue_crawler" "nri_counties" {
  name          = "bronze-nri-counties"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/nri/counties/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

# Census (5)
resource "aws_glue_crawler" "census_b01001" {
  name          = "bronze-census-acs5-2022-b01001"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/census/acs5_2022_B01001/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "census_b15003" {
  name          = "bronze-census-acs5-2022-b15003"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/census/acs5_2022_B15003/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "census_b23025" {
  name          = "bronze-census-acs5-2022-b23025"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/census/acs5_2022_B23025/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "census_b19013" {
  name          = "bronze-census-acs5-2022-b19013"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/census/acs5_2022_B19013/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "census_b25077" {
  name          = "bronze-census-acs5-2022-b25077"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "${local.base}/census/acs5_2022_B25077/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}
