CREATE TABLE IF NOT EXISTS int_common_tenant (
  tenant_id VARCHAR(2147483647) NOT NULL,
  tenant_name VARCHAR(2147483647) NOT NULL,
  tenant_description VARCHAR(2147483647),
  tenant_status VARCHAR(2147483647) NOT NULL,
  tenant_created_at TIMESTAMP,
  tenant_updated_at TIMESTAMP,
  -- put here column definitions
  PRIMARY KEY(tenant_id) NOT ENFORCED
) DISTRIBUTED BY HASH(tenant_id) INTO 1 BUCKETS
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