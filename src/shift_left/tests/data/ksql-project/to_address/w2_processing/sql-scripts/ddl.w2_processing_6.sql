CREATE TABLE IF NOT EXISTS etf_returns_t (
    return_id STRING,
    tax_year STRING,
    business_id STRING,
    recipient_id STRING,
    correction_type STRING,
    filing_status_id STRING,
    pdf_status STRING,
    PRIMARY KEY (return_id) NOT ENFORCED
) DISTRIBUTED BY HASH(return_id) INTO 3 BUCKETS WITH (
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