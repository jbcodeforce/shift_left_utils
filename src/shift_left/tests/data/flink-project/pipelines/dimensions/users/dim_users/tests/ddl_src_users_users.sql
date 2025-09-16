CREATE TABLE IF NOT EXISTS src_users_users_ut (
  user_id STRING NOT NULL,
  user_name STRING,
  user_email STRING NOT NULL,
  group_id STRING,
  created_date STRING,
  is_active BOOLEAN,
  updated_at TIMESTAMP,
  PRIMARY KEY(user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 1 BUCKETS
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