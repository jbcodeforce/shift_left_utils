CREATE TABLE IF NOT EXISTS src_p3_tenants (
  `id` STRING NOT NULL,
  `name` STRING NOT NULL,
  `description` STRING,
  `created_at` TIMESTAMP(3) NOT NULL,
  `updated_at` TIMESTAMP(3) NOT NULL,
  `created_by` STRING NOT NULL,
  `updated_by` STRING NOT NULL,
  `status` STRING NOT NULL,
  -- put here column definitions
  PRIMARY KEY(id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
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