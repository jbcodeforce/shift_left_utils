CREATE STREAM db_test_struct WITH (
  KAFKA_TOPIC = 'stage.db_test_stream_struct',
  VALUE_FORMAT = 'JSON_SR'
) AS
SELECT
  STRUCT(
    " TARGET_ID " := SUBSTRING(
      recordData,
      INSTR(recordData, 'target_id=') + 10,
      INSTR(
        recordData,
        ' ',
        INSTR(recordData, 'target_id='),
        1
      ) - INSTR(recordData, 'target_id=') -10
    ),
    " PORT_ID " := SUBSTRING(
      recordData,
      INSTR(recordData, 'port_id=') + 8,
      INSTR(recordData, ',', INSTR(recordData, 'port_id='), 1) - INSTR(recordData, 'port_id=') -8
    ),
    " SPEED " := SUBSTRING(
      recordData,
      INSTR(recordData, 'speed=') + 6,
      INSTR(recordData, ',', INSTR(recordData, 'speed='), 1) - INSTR(recordData, 'speed=') -6
    )
  ) " tags ",
  SUBSTRING(recordData, 1, INSTR(recordData, ',') -1) SERVICE,
  SUBSTRING(
    recordData,
    INSTR(recordData, 'host=') + 5,
    INSTR(recordData, ',', INSTR(recordData, 'host='), 1) - INSTR(recordData, 'host=') -5
  ) HOST,
  SUBSTRING(
    recordData,
    INSTR(recordData, 'port_id=') + 8,
    INSTR(recordData, ',', INSTR(recordData, 'port_id='), 1) - INSTR(recordData, 'port_id=') -8
  ) PORT_ID,
  CAST(
    SUBSTRING(
      recordData,
      INSTR(recordData, 'speed=') + 6,
      INSTR(recordData, ',', INSTR(recordData, 'speed='), 1) - INSTR(recordData, 'speed=') -6
    ) AS INT
  ) SPEED,
  SUBSTRING(
    recordData,
    INSTR(recordData, 'target_id=') + 10,
    INSTR(
      recordData,
      ' ',
      INSTR(recordData, 'target_id='),
      1
    ) - INSTR(recordData, 'target_id=') -10
  ) TARGET_ID,
  REPLACE(
    SUBSTRING(
      recordData,
      INSTR(recordData, 'op_state=') + 9,
      INSTR(recordData, ',', INSTR(recordData, 'op_state='), 1) - INSTR(recordData, 'op_state=') -9
    ),
    '"',
    ''
  ) OP_STATE,
  CAST(
    SUBSTRING(
      recordData,
      INSTR(recordData, 'errors_tx=') + 10,
      INSTR(
        recordData,
        ',',
        INSTR(recordData, 'errors_tx='),
        1
      ) - INSTR(recordData, 'errors_tx=') -10
    ) AS DOUBLE
  ) ERRORS_TX,
  REPLACE(
    SUBSTRING(recordData, INSTR(recordData, ' ', -1, 1), 17),
    ' ',
    ''
  ) " RECORD_TIME "
FROM
  EQUIPMENT_STAGE_STREAM
WHERE
  SUBSTRING(recordData, 1, INSTR(recordData, ',') -1) = 'errdrop_7x50' EMIT CHANGES;