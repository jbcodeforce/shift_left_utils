CREATE TABLE IF NOT EXISTS form_w2_t (
    form_w2_id STRING,
    return_id STRING,
    employee_id STRING,
    employee_ssn STRING,
    PRIMARY KEY (form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);