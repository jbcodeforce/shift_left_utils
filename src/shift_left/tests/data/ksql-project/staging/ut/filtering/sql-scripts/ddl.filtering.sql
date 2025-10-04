CREATE TABLE IF NOT EXISTS orders (
    `order_id` STRING,
    `customer_id` STRING,
    `product_id` STRING,
    `amount` DECIMAL(10, 2),
    `timestamp` TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS WITH (
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