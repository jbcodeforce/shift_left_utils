CREATE TABLE IF NOT EXISTS int_p1_table_1 (
    id STRING NOT NULL,
    user_name STRING, 
    account_id STRING,
    account_name STRING,
    balance int,
    new_att int,
    PRIMARY KEY(id) NOT ENFORCED 
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS WITH (
   'kafka.retention.time' = '0',
   'changelog.mode' = 'upsert',
   'kafka.cleanup-policy'= 'compact',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
)