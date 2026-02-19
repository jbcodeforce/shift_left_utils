CREATE TABLE IF NOT EXISTS movements (
    person STRING,
    location STRING,
    PRIMARY KEY (person) NOT ENFORCED
) DISTRIBUTED BY HASH(person) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
)