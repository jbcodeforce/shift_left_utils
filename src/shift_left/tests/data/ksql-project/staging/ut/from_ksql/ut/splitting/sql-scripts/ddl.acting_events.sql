CREATE TABLE IF NOT EXISTS acting_events (
    name STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (name) NOT ENFORCED
) DISTRIBUTED BY HASH(name) INTO 1 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);