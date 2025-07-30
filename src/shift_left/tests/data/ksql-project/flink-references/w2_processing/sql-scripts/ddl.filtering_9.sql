CREATE TABLE IF NOT EXISTS "ETF_RECIPIENT_T" ( 
	"recipient_id" VARCHAR,
	email_address STRING,
	recipient_telephone_no STRING,
	fax_number STRING,
	PRIMARY KEY("recipient_id") NOT ENFORCED
) DISTRIBUTED BY HASH("recipient_id") INTO 1 BUCKET WITH (
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
);