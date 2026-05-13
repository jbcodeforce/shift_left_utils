create table if not exists sl_raw_users (
    user_id STRING,
    user_name STRING,
    user_email STRING,
    group_id STRING,
    tenant_id STRING,
    created_date STRING,
    is_active BOOLEAN
) distributed by hash(user_id) into 1 buckets
with (
    'changelog.mode' = 'append',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
)
