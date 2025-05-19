CREATE TABLE IF NOT EXISTS src_p3_users_ut (
  `user_id` STRING NOT NULL,  
  `tenant_id` STRING NOT NULL,
  `role_id` STRING NOT NULL,
  `status` STRING NOT NULL,
  -- put here column definitions
  PRIMARY KEY(tenant_id, user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(tenant_id, user_id) INTO 3 BUCKETS
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