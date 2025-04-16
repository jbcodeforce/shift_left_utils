create table if not exists src_p1_table_3 (
    account_id STRING,
    user_id STRING,
    account_name STRING,
    balance int,
    PRIMARY KEY(account_id) NOT ENFORCED 
) DISTRIBUTED BY HASH(account_id) INTO 1 BUCKETS WITH (
   'kafka.retention.time' = '0',
   'changelog.mode' = 'append',
   'kafka.cleanup-policy'= 'compact',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
);