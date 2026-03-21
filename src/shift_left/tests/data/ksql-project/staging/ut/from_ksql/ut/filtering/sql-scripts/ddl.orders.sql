CREATE TABLE IF NOT EXISTS orders (
    `order_id` STRING,
    `customer_id` STRING,
    `order_sum` DECIMAL(10, 2),
    `order_date` TIMESTAMP(3),
    `status` STRING,
    PRIMARY KEY (`order_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`order_id`) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS filtered_orders (
    `order_id` STRING,
    `customer_id` STRING,
    `order_sum` DECIMAL(10, 2),
    `order_date` TIMESTAMP(3),
    `status` STRING,
    PRIMARY KEY (`order_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`order_id`) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);