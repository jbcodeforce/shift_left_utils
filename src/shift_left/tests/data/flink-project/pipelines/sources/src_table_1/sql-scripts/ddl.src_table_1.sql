-- user information
create table if not exists src_table_1 (
  user_id STRING NOT NULL,
  user_name STRING,
  PRIMARY KEY(user_id) NOT ENFORCED 
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS WITH (
   'kafka.retention.time' = '0',
   'changelog.mode' = 'append',
   'kafka.cleanup-policy'= 'compact',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
)
