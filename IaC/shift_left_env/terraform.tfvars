# Terraform Configuration for Healthcare Shift-Left Demo
# Using existing Confluent Cloud infrastructure

# Required: Confluent Cloud API credentials (for Terraform resource management)
confluent_cloud_api_key    = "B3KFLDE7J3SBYEHO"
confluent_cloud_api_secret = "cfltZ4+A69sHa+rP1dxfZohKaFhnV9NWdHXz2tAwJY0uLfyvZ2WsSGlNH+7sW6CQ"

service_account_id = "sa-111z1z"

# Cloud configuration (from CLOUD_PROVIDER / CLOUD_REGION)
cloud_provider = "AWS"
cloud_region   = "us-west-2"

# Resource naming
prefix = "sl"

flink_compute_pool_max_cfu = 30



