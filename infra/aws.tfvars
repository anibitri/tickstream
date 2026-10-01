# Optional one-off deploy of the AWS resources to a real account (Kafka and the
# services stay local). Check the current free-tier terms first, and run
# `terraform destroy -var-file=aws.tfvars` when you are done.
env = "aws-sandbox"

# Bucket names are global, so pick a unique one.
archive_bucket = "tickstream-archive-CHANGE-ME"

# This address gets an email when the bill reaches 80% of this monthly amount.
monthly_budget_usd = 5
budget_email       = "you@example.com"

# Point-in-time recovery is billed per GB; off keeps the sandbox free.
point_in_time_recovery = false

# Build the zip for the architecture you deploy (see `make lambda`).
lambda_architecture = "arm64"
