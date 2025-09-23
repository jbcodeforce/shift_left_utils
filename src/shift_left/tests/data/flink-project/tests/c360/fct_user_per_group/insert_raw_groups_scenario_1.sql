-- Integration test synthetic data for raw_groups
-- Generated on: 2025-09-22T23:43:40.084124+00:00
-- Unique test ID: 05fa4abd-487d-4db7-9137-b436b052ca04

INSERT INTO raw_groups_it (
    -- Add appropriate column names here
    id,
    test_unique_id,
    test_timestamp,
    -- Add other columns...
    created_at
) VALUES (
    -- Add appropriate test values here
    'test_id_1',
    '05fa4abd-487d-4db7-9137-b436b052ca04',
    TIMESTAMP '2025-09-22T23:43:40.084124+00:00',
    -- Add other test values...
    CURRENT_TIMESTAMP
);

-- Add more test records as needed
