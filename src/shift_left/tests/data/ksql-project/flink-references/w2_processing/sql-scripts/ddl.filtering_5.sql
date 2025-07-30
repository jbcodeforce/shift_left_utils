CREATE TABLE IF NOT EXISTS form_w2_t (
    form_w2_id STRING,
    return_id STRING,
    employee_id BIGINT,
    employee_ssn STRING,
    PRIMARY KEY(form_w2_id) NOT ENFORCED
)
DISTRIBUTED BY HASH(form_w2_id) INTO 1 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'execution-vertex.batch-size' = '3',
    'execution.parallelism' = '1',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'value.fields-include' = 'all'
);