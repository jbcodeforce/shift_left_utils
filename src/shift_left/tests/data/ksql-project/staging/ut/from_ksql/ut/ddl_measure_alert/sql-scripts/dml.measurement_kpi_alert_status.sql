INSERT INTO measurement_kpi_alert_status
SELECT 
    D.error_rate AS KPI_RESULT,
    D.*,
    C.*,
    CASE
        WHEN C.direction = 'ascending' AND (D.error_rate) >= C.highwatermark THEN 'CRITICAL'
        WHEN C.direction = 'ascending' AND ((D.error_rate) >= C.lowwatermark AND (D.error_rate)< C.highwatermark) THEN 'MAJOR'
        WHEN C.direction = 'ascending' AND (D.error_rate)< C.lowwatermark THEN 'OK'
        WHEN C.direction = 'descending' AND (D.error_rate) <= C.lowwatermark THEN 'CRITICAL'
        WHEN C.direction = 'descending' AND (D.error_rate) > C.lowwatermark AND (D.error_rate) <= C.highwatermark THEN 'MAJOR'
        WHEN C.direction = 'descending' AND (D.error_rate) > C.highwatermark THEN 'OK'
    END AS TCASTATUS
FROM measurement_stage_stream D
INNER JOIN kpi_config_table C
ON D.service = C.dbtable;