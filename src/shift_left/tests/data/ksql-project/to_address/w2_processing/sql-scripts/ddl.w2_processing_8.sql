CREATE TABLE IF NOT EXISTS etf_hub_users_t (
    return_id STRING,
    employee_id STRING,
    employee_ssn STRING,
    tax_year BIGINT,
    business_id STRING,
    structured ROW<
        submissionDetails ROW<
            taxYear BIGINT
        >,
        returnData ROW<
            business MAP<STRING, STRING>,
            employee ROW<
                employeeId STRING,
                ssn STRING
            >
        >
    >,
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