create table `src_order_raw_ut` (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  amount int,
  ts_ms bigint,
  shop_id STRING,
  PRIMARY KEY HASH(order_id) NOT ENFORCED 
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS WITH (
   'kafka.retention.time' = '0',
   'changelog.mode' = 'append',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
)

