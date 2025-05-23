CREATE TABLE IF NOT EXISTS `NOK_UVS_PM_STAGE_STREAM` (
   pmData STRING
) WITH (
   'connector' = 'kafka',
   'topic' = 'stage.uvs_pm_nok',
   'properties.bootstrap.servers' = 'localhost:9092',
   'format' = 'json'
)