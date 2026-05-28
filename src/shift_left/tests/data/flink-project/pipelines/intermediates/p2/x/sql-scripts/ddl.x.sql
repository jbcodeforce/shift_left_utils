CREATE TABLE IF NOT EXISTS x (
  default_key STRING,
  x_value STRING,
  -- put here column definitions
  PRIMARY KEY(default_key) NOT ENFORCED
) DISTRIBUTED BY HASH(default_key) INTO 1 BUCKETS
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
