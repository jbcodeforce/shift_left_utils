CREATE TABLE IF NOT EXISTS c360_dim_groups_ut (
  group_id STRING NOT NULL,
  tenant_id STRING NOT NULL,
  group_name STRING,
  group_type STRING,
  tenant_name STRING,
  created_date STRING,
  is_active BOOLEAN,
  updated_at TIMESTAMP,
  PRIMARY KEY(tenant_id, group_id) NOT ENFORCED
) DISTRIBUTED BY HASH(tenant_id, group_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);