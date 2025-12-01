CREATE TABLE IF NOT EXISTS filtered_orders (
    `order_id` BIGINT,
    `order_sum` DECIMAL(10,2)
    PRIMARY KEY (`order_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`order_id`) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.json-registry.schema-context' = '.flink-dev',
    'value.format' = 'json-registry',
    'key.format' = 'json-registry'
);
