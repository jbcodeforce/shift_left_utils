CREATE TABLE IF NOT EXISTS fct_user_per_group (
  group_id STRING NOT NULL,
  group_name STRING,
  group_type STRING,
  total_users BIGINT,
  active_users BIGINT,
  inactive_users BIGINT,
  latest_user_created_date BIGINT,
  fact_updated_at TIMESTAMP,
  PRIMARY KEY(group_id) NOT ENFORCED
) DISTRIBUTED BY HASH(group_id) INTO 1 BUCKETS
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