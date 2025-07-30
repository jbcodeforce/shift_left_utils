INSERT INTO equipment_stage_json_stream
SELECT
  -- Service
  TRIM(SUBSTR(recordData, 1, LENGTH(regexp_replace(recordData, '^([^,]+).*$', '\1'))) AS service,

  -- Host
  TRIM(REGEXP_REPLACE(regexp_extract(recordData, 'connector=([^,]*)', 1), '^' AS host