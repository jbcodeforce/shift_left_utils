CREATE TABLE IF NOT EXISTS basic_table_stream (
  kpiName STRING,
  kpiStatus STRING,
  highMetricThreshold INT,
  lowMetricThreshold INT,
  networkService STRING,
  elementType STRING,
  interfaceName STRING,
  dbTable STRING,
  PRIMARY KEY(kpiName) NOT ENFORCED
) DISTRIBUTED BY HASH(kpiName) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);