CREATE TABLE IF NOT EXISTS rock_songs (
    artist STRING,
    title STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);