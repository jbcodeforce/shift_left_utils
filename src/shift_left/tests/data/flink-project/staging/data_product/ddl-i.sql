CREATE TABLE IF NOT EXISTS `73XX_MEASUREMENT_KPI_ALERT_STATUS` WITH ( 
   'connector' = 'kafka',
   'topic' = 'stage.uvs_pm_nok_kpi_alert_73xx',
   'properties.bootstrap.servers' = 'localhost:9092',
   'format' = 'json'
) AS
SELECT  D.`ERRORS_RX_PCT`/1.0 AS `KPI_RESULT`, D.*, C.*,
	CASE
		WHEN    C.`DIRECTION` = 'ascending'
			AND (D.`ERRORS_RX_PCT`/1.0) >= C.`HIGHWATERMARK`
			THEN 'CRITICAL'
		WHEN     C.`DIRECTION` = 'ascending'
			AND ((D.`ERRORS_RX_PCT`/1.0) >= C.`LOWWATERMARK` AND (D.`ERRORS_RX_PCT`/1.0) < C.`HIGHWATERMARK`)
			THEN 'MAJOR'
		WHEN     C.`DIRECTION` =  'ascending'
			AND (D.`ERRORS_RX_PCT`/1.0) < C.`LOWWATERMARK`
			THEN 'OK'
		WHEN    C.`DIRECTION` = 'descending' 
            AND (D.`ERRORS_RX_PCT`/1.0) <= C.`LOWWATERMARK`  
            THEN 'CRITICAL'
        WHEN    C.`DIRECTION` = 'descending' 
            AND (D.`ERRORS_RX_PCT`/1.0) > C.`LOWWATERMARK`   AND (D.`ERRORS_RX_PCT`/1.0) <= C.`HIGHWATERMARK`
            THEN 'MAJOR'
		WHEN    C.`DIRECTION` = 'descending' 
			AND (D.`ERRORS_RX_PCT`/1.0) > C.`HIGHWATERMARK`
			THEN 'OK'
	END AS `TCASTATUS`
FROM   UVS_PM_NOK_STAGE_73XX_JSON_STREAM D
INNER JOIN KPI_CONFIG_TABLE C 
	ON D.`SERVICE` = C.`DBTABLE`
) PRIMARY KEY(sid) NOT ENFORCED -- VERIFY KEY
) DISTRIBUTED BY HASH(`sid`) INTO 6 BUCKETS WITH (
   'changelog.mode' = 'retract',
   'kafka.cleanup-policy'= 'compact',
   'key.avro-registry.schema-context' = '.flink-dev',
   'value.avro-registry.schema-context' = '.flink-dev',
   'key.format' = 'avro-debezium-registry',
   'value.format' = 'avro-debezium-registry',
   'kafka.retention.time' = '0',
   'kafka.producer.compression.type' = 'snappy',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'value.fields-include' = 'all'
)