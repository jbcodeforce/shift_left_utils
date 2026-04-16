# Terraform configuration for healthcare-shift-left-demo
# Confluent Cloud: environment, Kafka cluster, Flink compute pool

terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.57.0"
    }
  }
}

# Resource-specific metadata (Kafka REST, Schema Registry, Flink, Tableflow) must not be set on the
# provider when values would create a dependency cycle (Flink) or when sourced from backend/.env:
# the Confluent provider merges CONFLUENT_*, FLINK_*, KAFKA_*, SCHEMA_REGISTRY_*, etc. via
# EnvDefaultFunc. Any *partial* group triggers "all or none" validation errors.
# Explicit "" overrides process env; resources (e.g. confluent_flink_statement) pass credentials inline.
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret

  kafka_id            = ""
  kafka_api_key       = ""
  kafka_api_secret    = ""
  kafka_rest_endpoint = ""

  schema_registry_id            = ""
  schema_registry_api_key       = ""
  schema_registry_api_secret    = ""
  schema_registry_rest_endpoint = ""
  catalog_rest_endpoint         = ""

  flink_api_key         = ""
  flink_api_secret      = ""
  flink_rest_endpoint   = ""
  organization_id       = ""
  environment_id        = ""
  flink_compute_pool_id = ""
  flink_principal_id    = ""
}

# Data sources for organization and Flink region

data "confluent_organization" "my_org" {}

data "confluent_flink_region" "flink_region" {
  cloud  = var.cloud_provider
  region = var.cloud_region
}

# Only fetched when using existing environment
data "confluent_environment" "existing" {
  count = local.use_existing_env ? 1 : 0
  id    = var.environment_id
}
