CREATE TABLE IF NOT EXISTS src_s1 (

  -- put here column definitions
  PRIMARY KEY(id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'key.avro-registry.schema-context' = 'dev-',
  'value.avro-registry.schema-context' = 'dev-'
);