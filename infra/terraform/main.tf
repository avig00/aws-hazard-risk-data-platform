terraform {
  required_version = ">= 1.2.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "aws-hazard-risk-data-platform"
      Phase       = "platform"
      Environment = "dev"
      Owner       = "vigamogh"
      ManagedBy   = "terraform"
    }
  }
}
