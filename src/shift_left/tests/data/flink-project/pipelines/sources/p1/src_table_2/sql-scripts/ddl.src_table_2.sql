CREATE TABLE IF NOT EXISTS src_table_2 (
    order_id STRING,
    product_id STRING,
    customer_id STRING,
    amount int,
    PRIMARY KEY(order_id) NOT ENFORCED 
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS WITH (
   'kafka.retention.time' = '0',
   'changelog.mode' = 'append',
   'kafka.cleanup-policy'= 'compact',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
)