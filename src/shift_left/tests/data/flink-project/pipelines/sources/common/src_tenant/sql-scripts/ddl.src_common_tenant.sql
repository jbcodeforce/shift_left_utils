CREATE TABLE IF NOT EXISTS sl_cmn_src_tenants (
  tenant_id VARCHAR(2147483647) NOT NULL,
  tenant_name VARCHAR(2147483647) NOT NULL,
  tenant_description VARCHAR(2147483647),
  tenant_status VARCHAR(2147483647) NOT NULL,
  tenant_created_at TIMESTAMP(3),
  tenant_updated_at TIMESTAMP(3),
  PRIMARY KEY(tenant_id) NOT ENFORCED
) DISTRIBUTED BY HASH(tenant_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
