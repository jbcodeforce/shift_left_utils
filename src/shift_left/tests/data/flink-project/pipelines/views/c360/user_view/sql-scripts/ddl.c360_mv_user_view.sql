CREATE MATERIALIZED TABLE c360_mv_user_view (
  user_id STRING NOT NULL,
  user_name STRING,
  user_email STRING,
  group_id STRING,
  tenant_id STRING,
  tenant_name STRING,
  group_name STRING,
  group_type STRING,
  created_date STRING,
  is_active BOOLEAN,
  PRIMARY KEY(tenant_id, user_id) NOT ENFORCED
)
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
