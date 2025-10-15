CREATE TABLE IF NOT EXISTS app_orders (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    customer_id STRING,
    sum_order_amount DECIMAL(38, 18),
    PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS WITH (
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