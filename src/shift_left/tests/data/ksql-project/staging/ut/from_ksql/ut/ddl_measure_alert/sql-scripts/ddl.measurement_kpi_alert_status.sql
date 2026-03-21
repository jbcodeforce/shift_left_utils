CREATE TABLE IF NOT EXISTS measurement_kpi_alert_status (
    error_rate DOUBLE,
    service STRING,
    dbtable STRING,
    direction STRING,
    highwatermark DOUBLE,
    lowwatermark DOUBLE,
    tcastatus STRING,
    PRIMARY KEY (service) NOT ENFORCED
) DISTRIBUTED BY HASH(service) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);