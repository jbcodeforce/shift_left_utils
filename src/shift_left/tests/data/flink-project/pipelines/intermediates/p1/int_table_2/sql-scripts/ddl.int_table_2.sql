 create table if not exists (
  product_id STRING,
  product STRING,
 PRIMARY KEY HASH(id) NOT ENFORCED 
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS WITH (
   'kafka.retention.time' = '0',
   'changelog.mode' = 'retract',
   'kafka.cleanup-policy'= 'compact',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
)
