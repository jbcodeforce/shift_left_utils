CREATE TABLE IF NOT EXISTS orders (
    `order` STRING,
    order_sum BIGINT,
    PRIMARY KEY (`order`) NOT ENFORCED
) DISTRIBUTED BY HASH(`order`) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS filtered_orders (
    `order` STRING,
    order_sum BIGINT,
    PRIMARY KEY (`order`) NOT ENFORCED
) DISTRIBUTED BY HASH(`order`) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);