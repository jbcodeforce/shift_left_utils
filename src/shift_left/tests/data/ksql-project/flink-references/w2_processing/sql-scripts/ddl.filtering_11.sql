CREATE TABLE IF NOT EXISTS W2_RETURNS2_T (
    `submissionDetails.taxYear` BIGINT,
    `returnData.business` ROW<businessId STRING, email STRING>,
    `returnData.employee` ROW<employeeId STRING, ssn VARCHAR(11)>,
    PRIMARY KEY (`submissionDetails.taxYear`) NOT ENFORCED
)
DISTRIBUTED BY HASH (`submissionDetails.taxYear`) INTO 1 BUCKETS
WITH (
      'changelog.mode' = 'append', 
      'key.avro-registry.schema-context' = '.flink-dev',
      'value.avro-registry.schema-context' = '.flink-dev',
      'kafka.retention.time' = '0',
      'kafka.producer.compression.type' = 'snappy',
      'scan.bounded.mode' = 'unbounded',
      'scan.startup.mode' = 'earliest-offset',
      'value.fields-include' = 'all'
)