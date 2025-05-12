CREATE TABLE IF NOT EXISTS user_role (
  `user_sid` STRING,
  `user_role_sid` STRING,
  `user_id` STRING,
  `role_id` STRING,
  `role_name` STRING,
  `role_description` STRING,
  `tenant_id` STRING,
  PRIMARY KEY(`sid`) NOT ENFORCED -- VERIFY KEY
) WITH ( 
   'changelog.mode' = 'retract',
   'kafka.cleanup-policy'= 'compact',
   'key.avro-registry.schema-context' = '.flink-dev',
   'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'sql.local-time-zone' = 'UTC',
   'value.fields-include' = 'all'
)