CREATE TABLE IF NOT EXISTS int_p3_user_role (
  `id` VARCHAR(2147483647) NOT NULL,
  `key` VARCHAR(2147483647) NOT NULL,
  `value` VARCHAR(2147483647) NOT NULL,
  `tenant_id` VARCHAR(2147483647) NOT NULL,
  `description` VARCHAR(2147483647),
  `parent_id` VARCHAR(2147483647),
  `hierarchy` VARCHAR(2147483647),
  `op` VARCHAR(2147483647) NOT NULL,
  `source_lsn` BIGINT,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`id`, `tenant_id`) NOT ENFORCED
)
DISTRIBUTED BY HASH(`id`, `tenant_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);