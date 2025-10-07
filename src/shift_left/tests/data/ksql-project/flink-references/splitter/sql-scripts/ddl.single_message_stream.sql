CREATE TABLE IF NOT EXISTS single_message_stream (
    message_id STRING primary key not enforced,
    message STRING
) DISTRIBUTED BY HASH(message_id) INTO 1 BUCKETS WITH (
    'key.json-registry.schema-context' = '.flink-dev',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.format' = 'json-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);