CREATE TABLE IF NOT EXISTS KPI_CONFIG_TABLE WITH (
  dbTable AS (
    SELECT
      *
    FROM
      dbTable
  )
) AS
SELECT
  kpiName,
  kpiStatus,
  networkService,
  elementType,
  nfCode,
  dataCollector,
  reportingEntityName,
  DATABASE,
  retentionPolicy,
  tagSet,
  numerator,
  denominator,
  `where`,
  thresholdId,
  tcaStatus,
  highWatermark,
  lowWatermark,
  direction,
  alertDescription,
  prodAlert,
  alertType,
  rubyService,
  setMajorSuppressionPeriod,
  contCriticalSuppressionPeriod,
  contMajorSuppressionPeriod,
  clearSuppressionPeriod,
  processingType,
  kpiId,
  groupBy,
  setCriticalSuppressionPeriod,
  outputTopic,
  interfaceName,
  sourceName
FROM
  KPI_CONFIG_STREAM
GROUP
  BY dbTable PRIMARY KEY(sid) NOT ENFORCED -- VERIFY KEY ) DISTRIBUTED BY HASH(`sid`) INTO 6 BUCKETS WITH ( 'changelog.mode' = 'retract', 'kafka.cleanup-policy'= 'compact', 'key.avro-registry.schema-context' = '.flink-dev', 'value.avro-registry.schema-context' = '.flink-dev', 'key.format' = 'avro-debezium-registry', 'value.format' = 'avro-debezium-registry', 'kafka.retention.time' = '0', 'kafka.producer.compression.type' = 'snappy', 'scan.bounded.mode' = 'unbounded', 'scan.startup.mode' = 'earliest-offset', 'value.fields-include' = 'all')