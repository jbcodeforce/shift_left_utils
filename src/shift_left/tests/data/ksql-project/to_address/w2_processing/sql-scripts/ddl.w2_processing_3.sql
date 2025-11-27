CREATE TABLE IF NOT EXISTS etf_hub_users (
    user_id STRING,
    email_address STRING,
    contact_name STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 3 BUCKETS WITH (
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