# On-demand tables: no capacity to manage and no cost when idle.

resource "aws_dynamodb_table" "latest_metrics" {
  #checkov:skip=CKV_AWS_119:AWS-owned encryption key instead of a customer-managed KMS key ($1/month).
  name         = "latest_metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "symbol"
  range_key    = "window_secs"

  attribute {
    name = "symbol"
    type = "S"
  }
  attribute {
    name = "window_secs"
    type = "N"
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }
  server_side_encryption {
    enabled = true
  }
}

resource "aws_dynamodb_table" "alerts" {
  #checkov:skip=CKV_AWS_119:AWS-owned encryption key instead of a customer-managed KMS key ($1/month).
  name         = "alerts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "symbol"
  range_key    = "sk" # "<window_end_ns>#<alert_id>", so a symbol's alerts sort by time

  attribute {
    name = "symbol"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at" # alerts are deleted after 30 days
    enabled        = true
  }
  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }
  server_side_encryption {
    enabled = true
  }
}

resource "aws_dynamodb_table" "feed_health" {
  #checkov:skip=CKV_AWS_119:AWS-owned encryption key instead of a customer-managed KMS key ($1/month).
  name         = "feed_health"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "exchange"
  range_key    = "symbol"

  attribute {
    name = "exchange"
    type = "S"
  }
  attribute {
    name = "symbol"
    type = "S"
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }
  server_side_encryption {
    enabled = true
  }
}
