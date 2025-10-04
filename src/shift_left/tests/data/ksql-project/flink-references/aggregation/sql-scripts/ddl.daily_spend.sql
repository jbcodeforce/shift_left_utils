CREATE TABLE IF NOT EXISTS daily_spend (
    customer_id STRING,
    order_amount DECIMAL(10,2),
    category STRING,
    transaction_id STRING,
    tx_timestamp TIMESTAMP(3),
    WATERMARK FOR tx_timestamp AS tx_timestamp - INTERVAL '5' SECOND,
    PRIMARY KEY (`transaction_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`transaction_id`) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.json-registry.schema-context' = '.flink-dev',
    'value.json-registry.schema-context' = '.flink-dev',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json-registry',
    'value.fields-include' = 'all'
);