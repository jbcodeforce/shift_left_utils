CREATE TABLE IF NOT EXISTS db_test_stream_sr WITH ( 
   'connector' = 'kafka',
   'topic' = 'stage.test_db_stream',
   'properties.bootstrap.servers' = 'localhost:9092',
   'format' = 'json'
) AS SELECT
   map('TARGET_ID':=SUBSTRING(pmData,INSTR(pmData,'target_id=') +10,INSTR(pmData, ' ',INSTR(pmData,'target_id='), 1) - INSTR(pmData,'target_id=') -10),
   'PORT_ID':=SUBSTRING(pmData,INSTR(pmData,'port_id=') +8,INSTR(pmData, ',',INSTR(pmData,'port_id='), 1) - INSTR(pmData,'port_id=') -8),
   'SPEED':=SUBSTRING(pmData,INSTR(pmData,'speed=') +6,INSTR(pmData, ',',INSTR(pmData,'speed='), 1) - INSTR(pmData,'speed=') -6)) AS `tags`,
   SUBSTRING(pmData,1,INSTR(pmData, ',') -1) AS SERVICE,
   SUBSTRING(pmData,INSTR(pmData,'host=') +5,INSTR(pmData, ',',INSTR(pmData,'host='), 1) - INSTR(pmData,'host=') -5) AS HOST,
   SUBSTRING(pmData,INSTR(pmData,'port_id=') +8,INSTR(pmData, ',',INSTR(pmData,'port_id='), 1) - INSTR(pmData,'port_id=') -8) AS PORT_ID,
   CAST(SUBSTRING(pmData,INSTR(pmData,'speed=') +6,INSTR(pmData, ',',INSTR(pmData,'speed='), 1) - INSTR(pmData,'speed=') -6) AS INT) AS SPEED,
   SUBSTRING(pmData,INSTR(pmData,'target_id=') +10,INSTR(pmData, ' ',INSTR(pmData,'target_id='), 1) - INSTR(pmData,'target_id=') -10) AS TARGET_ID,
   REPLACE(SUBSTRING(pmData,INSTR(pmData,'op_state=') +9,INSTR(pmData, ',',INSTR(pmData,'op_state='), 1) - INSTR(pmData,'op_state=') -9),'