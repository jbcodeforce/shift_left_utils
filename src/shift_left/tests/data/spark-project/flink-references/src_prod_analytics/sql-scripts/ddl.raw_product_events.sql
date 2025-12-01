CREATE TABLE IF NOT EXISTS raw_product_events (
  product_id STRING,
  category STRING,
  event_timestamp TIMESTAMP(3),
  user_id STRING,
  event_type STRING,
  revenue DECIMAL(10, 2),
  PRIMARY KEY (product_id) NOT ENFORCED
) DISTRIBUTED BY HASH(product_id) INTO 1 BUCKETS WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'key.json-registry.schema-context' = '.flink-dev',
  'value.json-registry.schema-context' = '.flink-dev',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry'
)