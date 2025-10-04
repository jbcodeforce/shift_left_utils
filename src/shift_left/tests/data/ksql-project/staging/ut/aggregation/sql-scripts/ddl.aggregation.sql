CREATE TABLE IF NOT EXISTS daily_spend (
    `time` TIMESTAMP(3),
    user_id STRING,
    amount DECIMAL(10,2),
    category STRING,
    transaction_id STRING,
    PRIMARY KEY (`time`) NOT ENFORCED
) DISTRIBUTED BY HASH(`time`) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);