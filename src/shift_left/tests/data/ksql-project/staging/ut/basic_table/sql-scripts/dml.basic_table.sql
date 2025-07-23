CREATE TABLE IF NOT EXISTS basic_table_stream (
  `kpiName` STRING,
  `kpiStatus` STRING,
  highMetricThreshold INT,
  lowMetricThreshold INT
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);