CREATE TABLE IF NOT EXISTS KPI_CONFIG_TABLE (
    `dbTable` STRING,
    kpiName STRING,
    kpiStatus STRING,
    networkService STRING,
    elementType STRING,
    interfaceName STRING,
    PRIMARY KEY(`dbTable`) NOT ENFORCED
) DISTRIBUTED BY HASH(`dbTable`) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.format' = 'json-registry',
    'value.format' = 'json-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);