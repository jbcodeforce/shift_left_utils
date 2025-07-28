CREATE TABLE IF NOT EXISTS ETF_HUB_BUSINESS (
    business_id STRING,
    dba_name STRING ,
    user_id STRING,
    recipient_id INT,
    email_address STRING,
) DISTRIBUTED BY HASH(business_id) INTO 1 BUCKETS PRIMARY KEY (business_id) NOT ENFORCED WITH (
        'changelog.mode'='append',
        'key.avro-registry.schema-context'='.flink-dev',
        'value.avro-registry.schema-context'='.flink-dev',
        'key.format'='avro-registry',
        'value.format'='avro-registry',
        'kafka.retention.time'='0',
        'kafka.producer.compression.type'='snappy',
        'scan.bounded.mode'='unbounded',
        'scan.startup.mode'='earliest-offset',
        'value.fields-include'='all'
    );