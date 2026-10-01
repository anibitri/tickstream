# One configuration for both targets:
#   LocalStack:  terraform apply -var-file=local.tfvars
#   Real AWS:    terraform apply -var-file=aws.tfvars   (optional, see aws.tfvars)
# Kafka and the Go services always run locally; only the AWS resources differ.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

locals {
  local = var.localstack_endpoint != ""
  name  = "tickstream"
  tags = {
    project = "tickstream"
    env     = var.env
    owner   = var.owner
  }
}

provider "aws" {
  region = var.region

  # LocalStack accepts any credentials; real AWS uses your normal AWS login.
  access_key                  = local.local ? "test" : null
  secret_key                  = local.local ? "test" : null
  skip_credentials_validation = local.local
  skip_metadata_api_check     = local.local
  skip_requesting_account_id  = local.local
  s3_use_path_style           = local.local

  dynamic "endpoints" {
    for_each = local.local ? [1] : []
    content {
      s3       = var.localstack_endpoint
      dynamodb = var.localstack_endpoint
      sqs      = var.localstack_endpoint
      sns      = var.localstack_endpoint
      lambda   = var.localstack_endpoint
      iam      = var.localstack_endpoint
      sts      = var.localstack_endpoint
      logs     = var.localstack_endpoint
    }
  }

  default_tags {
    tags = local.tags
  }
}

data "aws_caller_identity" "current" {}
