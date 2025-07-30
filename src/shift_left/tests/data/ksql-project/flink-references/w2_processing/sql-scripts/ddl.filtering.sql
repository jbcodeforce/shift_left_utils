CREATE TABLE IF NOT EXISTS FORM_W2 (
    form_w2_id INT PRIMARY KEY,
    return_id BIGINT,
    employee_id BIGINT,
    employee_ssn STRING,
    WATERMARK FOR employee_ssn AS TIMESTAMPDIFF(SECOND, 5, PROCTIME()),
) 
DISTRIBUTED BY HASH(form_w2_id) INTO 1 BUCKETS
WITH (
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