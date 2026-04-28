# Confluent Cloud environment: create new or use existing (via environment_id variable)

locals {
  use_existing_env = var.environment_id != null && var.environment_id != ""
}

resource "confluent_environment" "env" {
  count        = local.use_existing_env ? 0 : 1
  display_name = "${var.prefix}-env"
  stream_governance {
    package = "ESSENTIALS"
  }
}

locals {
  environment_id   = local.use_existing_env ? var.environment_id : confluent_environment.env[0].id
  env_display_name = local.use_existing_env ? (length(data.confluent_environment.existing) > 0 ? data.confluent_environment.existing[0].display_name : "") : confluent_environment.env[0].display_name
  # Environment CRN (not SR cluster CRN): DataSteward is bound at environment scope for Schema Registry access.
  environment_resource_name = local.use_existing_env ? data.confluent_environment.existing[0].resource_name : confluent_environment.env[0].resource_name
}
