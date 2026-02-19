-- Flink SQL translated from aggregation.ksql per ksql_fsql/translator.txt

-- DDL: source table (daily-spend)
CREATE TABLE IF NOT EXISTS `daily-spend` (
    customer_id STRING,
    order_amount DECIMAL(19, 2),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'app.dev.daily-spend',
    'value.format' = 'json-registry',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

-- DDL: sink table (orders) with window columns
CREATE TABLE IF NOT EXISTS `orders` (
    customer_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    order_amount DECIMAL(19, 2),
    PRIMARY KEY (customer_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'app.dev.orders',
    'value.format' = 'json-registry',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

-- DML: continuous aggregation (filter then tumble then group)
INSERT INTO `orders`
SELECT
    customer_id,
    window_start,
    window_end,
    SUM(order_amount) AS order_amount
FROM TABLE(
    TUMBLE(
        (SELECT * FROM `daily-spend` WHERE order_amount > 0),
        DESCRIPTOR(event_time),
        INTERVAL '24' HOUR
    )
)
GROUP BY window_start, window_end, customer_id;
