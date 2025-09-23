-- Integration test synthetic data for raw_tenants
-- Generated on: 2025-09-22T23:43:40.084214+00:00
-- Unique test ID: 971993a9-558f-4927-b7f7-020707f1b505

INSERT INTO raw_tenants_it (
    -- Add appropriate column names here
    id,
    test_unique_id,
    test_timestamp,
    -- Add other columns...
    created_at
) VALUES (
    -- Add appropriate test values here
    'test_id_1',
    '971993a9-558f-4927-b7f7-020707f1b505',
    TIMESTAMP '2025-09-22T23:43:40.084214+00:00',
    -- Add other test values...
    CURRENT_TIMESTAMP
);

-- Add more test records as needed
