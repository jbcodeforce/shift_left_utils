CREATE TABLE IF NOT EXISTS engineers (
    engineer_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    PRIMARY KEY (engineer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(engineer_id) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS tickets (
    ticket_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    PRIMARY KEY (ticket_id) NOT ENFORCED
) DISTRIBUTED BY HASH(ticket_id) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS destinations (
    engineer_id STRING,
    longitude DOUBLE,
    latitude DOUBLE,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (engineer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(engineer_id) INTO 1 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);