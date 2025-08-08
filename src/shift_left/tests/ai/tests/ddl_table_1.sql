CREATE TABLE IF NOT EXISTS table_1 (
    sid                 STRING NOT NULL,
    column_1          STRING NOT NULL,
    column_2          STRING,
    column_3           CHAR,
    column_4             STRING,
    column_5               STRING,
    column_6             STRING,
    created_by         STRING,
    created_date       BIGINT,
    last_modified_by   STRING,
    last_modified_date BIGINT,
    description        STRING,
    is_active     BOOLEAN,
    PRIMARY KEY(sid, column_1) NOT ENFORCED
) DISTRIBUTED BY HASH(sid, column_1) INTO 6 BUCKETS WITH ( 
   'key.avro-registry.schema-context' = '.flink-dev',
   'value.avro-registry.schema-context' = '.flink-dev',
   'changelog.mode' = 'upsert',
   'kafka.retention.time' = '0',
   'kafka.producer.compression.type'='snappy',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'value.fields-include' = 'all',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry'
)

