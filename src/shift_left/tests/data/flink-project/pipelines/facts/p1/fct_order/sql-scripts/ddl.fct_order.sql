CREATE TABLE IF NOT EXISTS  fct_order(
    id STRING NOT NULL,
    customer_name STRING,
    account_name STRING,
    balance int,
    new_att int,
    PRIMARY KEY(id) NOT ENFORCED
) 
DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.cleanup-policy'= 'compact',
  'kafka.retention.time' = '0',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);