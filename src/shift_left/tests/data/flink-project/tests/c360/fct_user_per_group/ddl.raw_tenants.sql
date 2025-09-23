CREATE TABLE `j9r-env`.`j9r-kafka`.`raw_tenants_it` (
  `key` VARBINARY(2147483647),
  `source` ROW<`version` VARCHAR(2147483647), `name` VARCHAR(2147483647), `server_id` INT, `db` VARCHAR(2147483647), `table` VARCHAR(2147483647), `snapshot` BOOLEAN>,
  `op` VARCHAR(2147483647),
  `ts_ms` TIMESTAMP(3),
  `before` ROW<`tenant_id` VARCHAR(2147483647), `tenant_name` VARCHAR(2147483647), `tenant_description` VARCHAR(2147483647), `tenant_status` VARCHAR(2147483647), `tenant_created_at` VARCHAR(2147483647), `tenant_updated_at` VARCHAR(2147483647)>,
  `after` ROW<`tenant_id` VARCHAR(2147483647), `tenant_name` VARCHAR(2147483647), `tenant_description` VARCHAR(2147483647), `tenant_status` VARCHAR(2147483647), `tenant_created_at` VARCHAR(2147483647), `tenant_updated_at` VARCHAR(2147483647)>
)
DISTRIBUTED BY HASH(`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.compaction.time' = '0 ms',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.producer.compression.type' = 'snappy',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '0 ms',
  'key.format' = 'avro-registry',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'value.format' = 'avro-registry'
)
