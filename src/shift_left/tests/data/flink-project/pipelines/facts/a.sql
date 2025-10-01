CREATE TABLE IF NOT EXISTS user_risk_features (
   `user_id` STRING NOT NULL PRIMARY KEY NOT ENFORCED,
   `total_transactions` INT,
   `total_spending` DOUBLE,
   `avg_transaction_amount` DOUBLE,
   `spending_volatility` DOUBLE,
   `high_risk_transactions` INT,
   `avg_anomaly_score` DOUBLE,
   `rapid_spending_rate` DOUBLE,
   `spending_diversity` INT,
   `risk_pattern_diversity` INT,
   `final_user_risk_category` STRING,
   `spending_behavior_type` STRING 
) DISTRIBUTED BY HASH(user_id) INTO 1 BUCKETS WITH (
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