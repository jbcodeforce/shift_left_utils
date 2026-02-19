CREATE TABLE IF NOT EXISTS rock_songs (
    artist STRING,
    title STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 1 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS classical_songs (
    artist STRING,
    title STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 1 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS all_songs (
    artist STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 1 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
)