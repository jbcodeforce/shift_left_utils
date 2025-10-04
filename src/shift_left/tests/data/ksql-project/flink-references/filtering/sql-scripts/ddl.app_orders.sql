CREATE TABLE IF NOT EXISTS app_orders (
    `order_id` STRING,
    `customer_id` STRING,
    `product_id` STRING,
    `amount` DECIMAL(10, 2),
    `order_ts` TIMESTAMP(3),
    WATERMARK FOR order_ts AS order_ts - INTERVAL '5' SECOND,
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.json-registry.schema-context' = '.flink-dev',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.format' = 'json-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);