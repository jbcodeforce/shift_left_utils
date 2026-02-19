CREATE TABLE IF NOT EXISTS basic_table_stream (
    kpiName STRING,
    kpiStatus STRING,
    highMetricThreshold INT,
    lowMetricThreshold INT,
    networkService STRING,
    elementType STRING,
    interfaceName STRING,
    dbTable STRING,
    PRIMARY KEY (kpiName) NOT ENFORCED
) DISTRIBUTED BY HASH(kpiName) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);