CREATE TABLE IF NOT EXISTS measurement_kpi_alert_status (
    kpi_result DOUBLE,
    service STRING,
    error_rate DOUBLE,
    dbtable STRING,
    direction STRING,
    lowwatermark DOUBLE,
    highwatermark DOUBLE,
    tcastatus STRING,
    PRIMARY KEY (service) NOT ENFORCED
) DISTRIBUTED BY HASH(service) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.format' = 'json-registry',
    'value.format' = 'json-registry'
);