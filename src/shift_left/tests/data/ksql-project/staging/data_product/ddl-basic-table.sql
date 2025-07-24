create table if not exists basic_table_stream (
    kpiName string,
    kpiStatus string,
    highMetricThreshold int,
    lowMetricThreshold int,
) distributed by hash(kpiName) into 1 buckets with (
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.format' = 'json-registry',
    'value.format' = 'json-registry',
    'value.fields-include' = 'all',
    'scan.startup.mode' = 'earliest-offset'
);