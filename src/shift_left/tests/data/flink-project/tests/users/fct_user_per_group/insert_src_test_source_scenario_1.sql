-- Integration test synthetic data for src_test_source
-- Generated on: 2025-09-16T18:20:19.444029+00:00
-- Unique test ID: d4a5ca08-7621-4b5b-aff8-56decc642976

-- Add synthetic data with unique identifiers and timestamps
-- Update the following INSERT statement with appropriate test data:

INSERT INTO src_test_source_it (
    -- Add appropriate column names here
    id,
    test_unique_id,
    test_timestamp,
    -- Add other columns...
    created_at
) VALUES (
    -- Add appropriate test values here
    'test_id_1',
    'd4a5ca08-7621-4b5b-aff8-56decc642976',
    TIMESTAMP '2025-09-16T18:20:19.444029+00:00',
    -- Add other test values...
    CURRENT_TIMESTAMP
);

-- Add more test records as needed
