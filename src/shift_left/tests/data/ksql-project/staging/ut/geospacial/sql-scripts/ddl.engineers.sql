CREATE TABLE IF NOT EXISTS engineers (
    engineers STRING,
    longitude DOUBLE,
    latitude DOUBLE,
    PRIMARY KEY (`engineers`) NOT ENFORCED
) DISTRIBUTED BY HASH(`engineers`) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.fields-include' = 'all',
    'scan.startup.mode' = 'earliest-offset'
);