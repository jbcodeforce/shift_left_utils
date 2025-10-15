CREATE TABLE IF NOT EXISTS filtered_orders (
    order_id STRING,
    customer_id STRING,
    order_sum DOUBLE,
    order_date TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);