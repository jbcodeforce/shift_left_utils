CREATE TABLE IF NOT EXISTS  p1_fct_order(
    id STRING NOT NULL,
    customer_name STRING,
    account_name STRING,
    balance int,
    PRIMARY KEY(id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.cleanup-policy'= 'compact',
  'kafka.retention.time' = '0', 
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);