CREATE TABLE IF NOT EXISTS daily_spend (
    customer_id STRING,
    order_amount DECIMAL(10, 2),
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);