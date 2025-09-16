-- Run the below command to apply the bulk ALTERs
-- shift_left pipeline prepare --compute-pool-id lfcp-rxp621 alter_table_avro_debezium_stage.sql
ALTER TABLE `clone.dev.user-tag` SET ( 'value.format' = 'avro-debezium-registry', 'changelog.mode' = 'upsert');
ALTER TABLE `clone.dev.state-link` SET ( 'value.format' = 'avro-debezium-registry', 'changelog.mode' = 'upsert');
