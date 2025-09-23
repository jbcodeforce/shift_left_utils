CREATE TABLE `j9r-env`.`j9r-kafka`.`raw_users_it` (
  `user_id` VARCHAR(2147483647),
  `user_name` VARCHAR(2147483647),
  `user_email` VARCHAR(2147483647),
  `group_id` VARCHAR(2147483647),
  `tenant_id` VARCHAR(2147483647),
  `created_date` VARCHAR(2147483647),
  `is_active` BOOLEAN
)
DISTRIBUTED BY HASH(`user_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.compaction.time' = '0 ms',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.producer.compression.type' = 'snappy',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '0 ms',
  'key.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.avro-registry.schema-context' = '.flink-dev',
  'value.fields-include' = 'all',
  'value.format' = 'avro-registry'
)
