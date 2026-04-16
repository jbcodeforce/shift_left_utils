# Terraform Configuration for Healthcare Shift-Left Demo
# Using existing Confluent Cloud infrastructure

# Required: Confluent Cloud API credentials (for Terraform resource management)

# Cloud configuration (from CLOUD_PROVIDER / CLOUD_REGION)
cloud_provider = "AWS"
cloud_region   = "us-west-2"

# Resource naming
prefix = "sl"

flink_compute_pool_max_cfu = 30



