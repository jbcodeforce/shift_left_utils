CREATE TABLE IF NOT EXISTS `NOK_UVS_PM_STAGE_STREAM` (
   pmData STRING
) DISTRIBUTED BY HASH(`sid`) INTO 6 BUCKETS WITH (
   PRIMARY KEY(sid) NOT ENFORCED -- VERIFY KEY,
   'changelog.mode' = 'append',
   'kafka.cleanup-policy'= 'compact',
   'key.avro-registry.schema-context' = '.flink-dev',
   'value.avro-registry.schema-context' = '.flink-dev',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'kafka.retention.time' = '0',
   'kafka.producer.compression.type' = 'snappy',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'value.fields-include' = 'all'
)
