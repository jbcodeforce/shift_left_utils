# -----------------------------------------------------------------------------
# Variables Configuration
# Confluent Cloud Infrastructure
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# General Settings
# -----------------------------------------------------------------------------
variable "prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "txp"
}

variable "owner_email" {
  description = "Email of the resource owner for tagging"
  type        = string
  default     = "jboyer@confluent.io"
}

# -----------------------------------------------------------------------------
# AWS Configuration
# -----------------------------------------------------------------------------
variable "cloud_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-west-2"
}

# -----------------------------------------------------------------------------
# Confluent Cloud Configuration - Better to have a service account for tf-runner with 
# specific API key and Secrets
# -----------------------------------------------------------------------------
variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key (can be set via CONFLUENT_CLOUD_API_KEY env var)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret (can be set via CONFLUENT_CLOUD_API_SECRET env var)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "cc_availability" {
  description = "Kafka cluster availability (SINGLE_ZONE or MULTI_ZONE)"
  type        = string
  default     = "SINGLE_ZONE"
}

# -----------------------------------------------------------------------------
# Existing Resource IDs (optional - if set, will reuse existing resources)
# -----------------------------------------------------------------------------
variable "existing_environment_id" {
  description = "Existing Confluent Cloud environment ID to reuse (leave empty to create new)"
  type        = string
  default     = null
}

variable "existing_environment_name" {
  description = "Existing Confluent Cloud environment name to reuse (leave empty to create new)"
  type        = string
  default     = null
}

variable "existing_kafka_cluster_id" {
  description = "Existing Kafka cluster ID to reuse (leave empty to create new)"
  type        = string
  default     = null
}

variable "existing_service_account_id" {
  description = "Existing service account ID to reuse (leave empty to create new)"
  type        = string
  default     = null
}

variable "existing_schema_registry_id" {
  description = "Existing Schema Registry cluster ID to reuse (leave empty to use auto-provisioned one)"
  type        = string
  default     = null
}


variable "create_compute_pool" {
  description = "Whether to create a new Flink compute pool. If false, use existing flink_compute_pool_id"
  type        = bool
  default     = false
}

variable "existing_flink_compute_pool_id" {
  description = "Existing Flink compute pool ID to reuse (leave empty to create new)"
  type        = string
  default     = null
}

variable "flink_max_cfu" {
  description = "Maximum number of Confluent Flink Units (CFUs) for the Flink compute pool. Valid values: 5, 10, 20, 30, 40, 50"
  type        = number
  default     = 50
}

variable "flink_api_key" {
  description = "Flink API Key ID from the flink_service_account_id (required if not using remote state). Should be an API key associated with flink_service_account_id."
  type        = string
  default     = ""
  sensitive   = true
}

variable "flink_api_secret" {
  description = "Flink API Secret from the flink_service_account_id (required if not using remote state)"
  type        = string
  default     = ""
  sensitive   = true
}

