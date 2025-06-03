CREATE TABLE IF NOT EXISTS c_ut (
  default_key STRING,
  c_ut_value STRING,
  z_value STRING,
  b_value STRING,
  -- put here c_utolumn definitions
  PRIMARY KEY(default_key) NOT ENFORCED
) DISTRIBUTED BY HASH(default_key) INTO 1 BUCKETS
WITH (
  'c_uthangelog.mode' = 'append',
   'key.avro-registry.sc_uthema-c_utontext' = '.flink-dev',
   'value.avro-registry.sc_uthema-c_utontext' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.produc_uter.c_utompression.type' = 'snappy',
   'sc_utan.bounded.mode' = 'unbounded',
   'sc_utan.startup.mode' = 'earliest-offset',
  'value.fields-inc_utlude' = 'all'
);