create table if not exists raw_tenants (
     `key` bytes,
     `source` ROW<
        version STRING,                   
        name STRING,     
        server_id INT,   
        db STRING,      
        `table` STRING,    
        snapshot BOOLEAN
    >,
    op STRING,
    ts_ms TIMESTAMP(3),
    -- Data: The current table record data
    `before` ROW<tenant_id STRING,
            tenant_name STRING,
            tenant_description STRING,
            tenant_status STRING,
            tenant_created_at STRING,
            tenant_updated_at STRING>,
    `after` ROW<tenant_id STRING,
            tenant_name STRING,
            tenant_description STRING,
            tenant_status STRING,
            tenant_created_at STRING,
            tenant_updated_at STRING>
) DISTRIBUTED BY HASH(key) INTO 1 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);