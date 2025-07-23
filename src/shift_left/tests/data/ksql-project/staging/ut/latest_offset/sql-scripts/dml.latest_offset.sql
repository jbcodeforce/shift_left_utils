CREATE TABLE IF NOT EXISTS KPI_CONFIG_TABLE (
  `dbTable` STRING PRIMARY KEY NOT ENFORCED,
  kpiName STRING,
  kpiStatus STRING,
  networkService STRING,
  elementType STRING,
  interfaceName STRING,
  sourceName STRING
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);