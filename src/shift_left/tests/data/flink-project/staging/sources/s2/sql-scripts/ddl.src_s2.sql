CREATE TABLE IF NOT EXISTS src_s2 (

  -- put here column definitions
  PRIMARY KEY(__db) NOT ENFORCED
) DISTRIBUTED BY HASH(__db) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);