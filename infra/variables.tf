variable "env" {
  description = "Environment name, used in tags (local or aws-sandbox)."
  type        = string
}

variable "owner" {
  description = "Owner tag on every resource."
  type        = string
  default     = "anibitri"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-2"
}

variable "localstack_endpoint" {
  description = "LocalStack URL, e.g. http://localhost:4566. Empty means real AWS."
  type        = string
  default     = ""
}

variable "archive_bucket" {
  description = "Name of the S3 bucket for the Parquet trade archive (must be globally unique on real AWS)."
  type        = string
  default     = "tickstream-archive"
}

variable "lambda_zip" {
  description = "Path to the alert-handler zip (built by `make lambda`)."
  type        = string
  default     = "../build/alert-handler.zip"
}

variable "lambda_architecture" {
  description = "Lambda CPU architecture: arm64 or x86_64 (must match how the zip was built)."
  type        = string
  default     = "arm64"
  validation {
    condition     = contains(["arm64", "x86_64"], var.lambda_architecture)
    error_message = "lambda_architecture must be arm64 or x86_64."
  }
}

variable "alert_webhook_url" {
  description = "Optional Discord/Slack webhook for alert notifications."
  type        = string
  default     = ""
  sensitive   = true
}

variable "point_in_time_recovery" {
  description = "Enable DynamoDB point-in-time recovery (backups)."
  type        = bool
  default     = true
}

variable "monthly_budget_usd" {
  description = "Create an AWS budget alert at this monthly cost (0 disables; LocalStack does not need it)."
  type        = number
  default     = 0
}

variable "budget_email" {
  description = "Email address for budget alerts."
  type        = string
  default     = ""
}
