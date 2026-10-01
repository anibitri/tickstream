# Parquet trade archive (trades/...) and backtest reports (backtests/...).

resource "aws_s3_bucket" "archive" {
  #checkov:skip=CKV_AWS_145:SSE-S3 (AES-256) instead of a customer-managed KMS key, which costs $1/month.
  #checkov:skip=CKV_AWS_144:Cross-region replication doubles storage cost; the archive can be rebuilt from exchanges.
  #checkov:skip=CKV_AWS_18:Access logging needs a second bucket; not worth it for a single-user archive.
  #checkov:skip=CKV2_AWS_62:No consumer needs S3 event notifications.
  bucket        = var.archive_bucket
  force_destroy = true # lets `terraform destroy` clean up a sandbox deploy
}

resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  #checkov:skip=CKV_AWS_145:SSE-S3 (AES-256) instead of a customer-managed KMS key, which costs $1/month.
  bucket = aws_s3_bucket.archive.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "trades-to-cheaper-storage"
    status = "Enabled"
    filter {
      prefix = "trades/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "expire-backtests"
    status = "Enabled"
    filter {
      prefix = "backtests/"
    }
    expiration {
      days = 90
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
