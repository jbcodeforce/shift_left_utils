-- Test SQL file for integration testing of prepare_tables_from_sql_file function
-- This file contains safe table preparation statements for testing

-- Comment line that should be skipped
ALTER TABLE `src_table_1` SET ('connector' = 'kafka');

-- Another comment with some details
ALTER TABLE `src_table_1` SET ('topic' = 'test-topic-1');

-- Set format properties
ALTER TABLE `src_table_1` SET ('value.format' = 'json');

-- This is a comment that should be ignored
