CREATE TABLE IF NOT EXISTS db_test_struct (
    tags ROW<` TARGET_ID ` STRING, ` PORT_ID ` STRING, ` SPEED ` STRING>,
    SERVICE STRING,
    HOST STRING,
    PORT_ID STRING,
    SPEED INT,
    TARGET_ID STRING,
    OP_STATE STRING,
    ERRORS_TX DOUBLE,
    ` RECORD_TIME ` STRING,
    PRIMARY KEY (SERVICE) NOT ENFORCED
) DISTRIBUTED BY HASH(SERVICE) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);