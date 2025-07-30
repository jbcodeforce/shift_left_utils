CREATE TABLE IF NOT EXISTS W2_RETURNS_T (
submissionDetails.STRUCT<taxYear INTEGER> ROWTIME,
returnData STRUCT<business MAP<STRING, STRING>, employee STRUCT<employeeId BIGINT, ssn VARCHAR>> ,
structured STRUCT<submissionDetails STRUCT<taxYear INTEGER>, returnData STRUCT<business MAP<STRING, STRING>, employee STRUCT<employeeId BIGINT, ssn VARCHAR>>> 
) PRIMARY KEY (submissionDetails) NOT ENFORCED,
) DISTRIBUTED BY HASH(submissionDetails) INTO 1 BUCKETS
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