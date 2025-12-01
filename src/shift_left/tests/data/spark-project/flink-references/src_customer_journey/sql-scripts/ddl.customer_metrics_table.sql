CREATE TABLE IF NOT EXISTS customer_metrics_table (
  customer_segment STRING,
  membership_tier STRING,
  location STRING,
  customer_count BIGINT,
  avg_sessions DOUBLE,
  avg_events_per_session DOUBLE,
  avg_total_spent DOUBLE,
  median_total_spent DOUBLE,
  recent_purchasers BIGINT,
  at_risk_customers BIGINT,
  PRIMARY KEY (customer_segment, membership_tier, location) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_segment, membership_tier, location) INTO 1 BUCKETS WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'key.json-registry.schema-context' = '.flink-dev',
  'value.json-registry.schema-context' = '.flink-dev',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry'
);