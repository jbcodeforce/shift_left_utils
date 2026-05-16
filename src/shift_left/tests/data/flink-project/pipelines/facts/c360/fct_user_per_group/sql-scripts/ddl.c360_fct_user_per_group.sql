CREATE TABLE IF NOT EXISTS sl_c360_fct_user_per_group (
  tenant_id STRING NOT NULL,
  group_id STRING NOT NULL,
  group_name STRING,
  total_users BIGINT,
  active_users BIGINT,
  inactive_users BIGINT,
  PRIMARY KEY(tenant_id,group_id) NOT ENFORCED
) DISTRIBUTED BY HASH(tenant_id,group_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
