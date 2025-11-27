CREATE TABLE IF NOT EXISTS single_message_stream (
    message STRING,
    PRIMARY KEY (message) NOT ENFORCED
) DISTRIBUTED BY HASH(message) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'value.fields-include' = 'all',
    'scan.startup.mode' = 'earliest-offset',
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);