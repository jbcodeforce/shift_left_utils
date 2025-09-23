-- Integration test synthetic data for raw_users
-- Generated on: 2025-09-22T23:43:40.084296+00:00
-- Unique test ID: 0b91b20e-1753-4f57-8fdb-b755dae14517

INSERT INTO raw_users_it (
    -- Add appropriate column names here
    id,
    test_unique_id,
    test_timestamp,
    -- Add other columns...
    created_at
) VALUES (
    -- Add appropriate test values here
    'test_id_1',
    '0b91b20e-1753-4f57-8fdb-b755dae14517',
    TIMESTAMP '2025-09-22T23:43:40.084296+00:00',
    -- Add other test values...
    CURRENT_TIMESTAMP
);

-- Add more test records as needed
