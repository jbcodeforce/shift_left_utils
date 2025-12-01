CREATE TABLE IF NOT EXISTS fct_users (
  user_id STRING,
  username STRING,
  email STRING,
  group_name STRING,
  group_type STRING,
  user_status STRING,
  last_login_date TIMESTAMP(3),
  created_date TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 1 BUCKETS WITH (
  'changelog.mode' = 'append',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'key.json-registry.schema-context' = '.flink-dev',
  'value.json-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
)
