CREATE TABLE IF NOT EXISTS raw_events (
  event_id STRING,
  user_id STRING,
  event_timestamp TIMESTAMP(3),
  event_type STRING,
  event_properties ROW<
    tags ARRAY<STRING>,
    custom_fields MAP<STRING, STRING>
  >,
  event_metadata ROW<
    device_info ROW<
      os STRING,
      browser STRING
    >,
    location ROW<
      country STRING,
      city STRING
    >
  >,
  raw_event_data STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) DISTRIBUTED BY HASH(event_id) INTO 1 BUCKETS WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'value.json-registry.schema-context' = '.flink-dev',
  'key.json-registry.schema-context' = '.flink-dev',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry'
)