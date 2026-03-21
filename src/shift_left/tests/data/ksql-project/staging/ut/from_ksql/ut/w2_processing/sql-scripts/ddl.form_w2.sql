CREATE TABLE IF NOT EXISTS form_w2 (
    form_w2_id INT,
    return_id BIGINT,
    employee_id BIGINT,
    employee_ssn STRING,
    PRIMARY KEY (form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_returns (
    return_id BIGINT,
    tax_year STRING,
    business_id STRING,
    recipient_id INT,
    correction_type STRING,
    filing_status_id STRING,
    pdf_status BOOLEAN,
    PRIMARY KEY (return_id) NOT ENFORCED
) DISTRIBUTED BY HASH(return_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_hub_business (
    business_id STRING,
    dba_name STRING,
    user_id STRING,
    recipient_id INT,
    email_address STRING,
    PRIMARY KEY (business_id) NOT ENFORCED
) DISTRIBUTED BY HASH(business_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_hub_users (
    user_id STRING,
    email_address STRING,
    contact_name STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_recipient (
    recipient_id INT,
    email_address STRING,
    recipient_telephone_no STRING,
    fax_number STRING,
    PRIMARY KEY (recipient_id) NOT ENFORCED
) DISTRIBUTED BY HASH(recipient_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS form_w2_t (
    form_w2_id INT,
    return_id BIGINT,
    employee_id BIGINT,
    employee_ssn STRING,
    PRIMARY KEY (form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_returns_t (
    return_id BIGINT,
    tax_year STRING,
    business_id STRING,
    recipient_id INT,
    correction_type STRING,
    filing_status_id STRING,
    pdf_status BOOLEAN,
    PRIMARY KEY (return_id) NOT ENFORCED
) DISTRIBUTED BY HASH(return_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_hub_business_t (
    business_id STRING,
    dba_name STRING,
    user_id STRING,
    recipient_id INT,
    email_address STRING,
    PRIMARY KEY (business_id) NOT ENFORCED
) DISTRIBUTED BY HASH(business_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_hub_users_t (
    user_id STRING,
    email_address STRING,
    contact_name STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS etf_recipient_t (
    recipient_id INT,
    email_address STRING,
    recipient_telephone_no STRING,
    fax_number STRING,
    PRIMARY KEY (recipient_id) NOT ENFORCED
) DISTRIBUTED BY HASH(recipient_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS w2_returns_t (
    form_w2_id INT,
    return_id BIGINT,
    employee_id BIGINT,
    employee_ssn STRING,
    tax_year STRING,
    business_id STRING,
    recipient_id INT,
    correction_type STRING,
    filing_status_id STRING,
    pdf_status BOOLEAN,
    structured ROW<
        submissionDetails ROW<taxYear STRING>,
        returnData ROW<
            business MAP<STRING, STRING>,
            employee ROW<employeeId BIGINT, ssn STRING>
        >
    >,
    PRIMARY KEY (form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
    'value.format' = 'avro-registry',
    'key.format' = 'avro-registry',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.avro-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS w2_returns2_t (
    form_w2_id INT,
    return_id BIGINT,
    employee_id BIGINT,
    employee_ssn STRING,
    tax_year STRING,
    business_id STRING,
    recipient_id INT,
    correction_type STRING,
    filing_status_id STRING,
    pdf_status BOOLEAN,
    structured ROW<
        submissionDetails ROW<taxYear STRING>,
        returnData ROW<
            business MAP<STRING, STRING>,
            employee ROW<employeeId BIGINT, ssn STRING>,
            states ARRAY<STRING>
        >
    >,
    PRIMARY KEY (form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
    'value.format' = 'json-registry',
    'key.format' = 'json-registry',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);