CREATE TABLE IF NOT EXISTS tickets (
    `id` STRING,
    `type` STRING,
    `status` STRING,
    longitude DOUBLE,
    latitude DOUBLE,
    `created_at` TIMESTAMP(3),
    `updated_at` TIMESTAMP(3),
    `details` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`id`) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
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