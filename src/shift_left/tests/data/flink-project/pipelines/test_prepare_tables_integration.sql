-- Test SQL file for integration testing of prepare_tables_from_sql_file function
-- This file contains safe table preparation statements for testing

-- Comment line that should be skipped
ALTER TABLE `orphan` SET ('changelog.mode' = 'upsert');

-- Another comment with some details
alter table orphan SET ('scan.startup.mode' = 'latest-offset')

-- Set format properties
ALTER TABLE `orphan` SET ('value.format' = 'json-registry');

ALTER TABLE orphan SET ('kafka.cleanup-policy' = 'compact');

-- Supported options:
--
-- changelog.mode
-- connector
-- error-handling.log.target
-- error-handling.mode
-- kafka.cleanup-policy
-- kafka.compaction.time
-- kafka.consumer.isolation-level
-- kafka.max-message-size
-- kafka.message-timestamp-type
-- kafka.producer.compression.type
-- kafka.retention.size
-- kafka.retention.time
-- key.avro-registry.id-encoding
-- key.avro-registry.schema-context
-- key.avro-registry.subject-names
-- key.fields-prefix
-- key.format
-- late-handling.mode
-- scan.bounded.mode
-- scan.bounded.specific-offsets
-- scan.bounded.timestamp-millis
-- scan.startup.mode
-- scan.startup.specific-offsets
-- scan.startup.timestamp-millis
-- value.avro-registry.id-encoding
-- value.avro-registry.schema-context
-- value.avro-registry.subject-names
-- value.fields-include
-- value.format
