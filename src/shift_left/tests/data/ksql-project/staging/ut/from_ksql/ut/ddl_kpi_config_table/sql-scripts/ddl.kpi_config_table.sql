CREATE TABLE IF NOT EXISTS kpi_config_table (
    dbtable STRING,
    dbtable0 STRING,
    kpiname STRING,
    kpistatus STRING,
    networkservice STRING,
    elementtype STRING,
    interfacename STRING,
    PRIMARY KEY (dbtable) NOT ENFORCED
) DISTRIBUTED BY HASH(dbtable) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);