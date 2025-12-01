CREATE TABLE IF NOT EXISTS enriched_product_events (
  product_id STRING NOT NULL PRIMARY KEY NOT ENFORCED,
  `category` STRING,
  event_timestamp TIMESTAMP(3),
  user_id STRING,
  event_type STRING,
  revenue DECIMAL(10, 2),
  latest_event_rank BIGINT,
  prev_revenue DECIMAL(10, 2),
  rolling_7day_revenue DECIMAL(10, 2),
  revenue_rank_in_category BIGINT,
  event_date STRING,
  event_hour INT,
  day_of_week INT,
  purchase_revenue DECIMAL(10, 2),
  time_category STRING
) DISTRIBUTED BY HASH(product_id) INTO 1 BUCKETS WITH (
   'changelog.mode' = 'append',
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
PRIMARY KEY(product_id) NOT ENFORCED -- VERIFY KEY