-- Test data for splitter logic
-- Testing CROSS JOIN UNNEST to split array of messages into individual rows
-- Expected results:
--   MSG001: 3 output rows (Hello, World, Test)
--   MSG002: 1 output row (Single message)
--   MSG003: 5 output rows (First, Second, Third, Fourth, Fifth)
--   MSG004: 2 output rows (Alert, Notification)
--   MSG005: 0 output rows (empty array)
--   MSG006: 4 output rows (Morning, Afternoon, Evening, Night)
-- Total expected output: 15 rows

INSERT INTO multi_message_stream VALUES 
    -- Test Case 1: Multiple messages (3 messages)
    ('MSG001', ARRAY['Hello', 'World', 'Test']),
    
    -- Test Case 2: Single message in array
    ('MSG002', ARRAY['Single message']),
    
    -- Test Case 3: Many messages (5 messages)
    ('MSG003', ARRAY['First', 'Second', 'Third', 'Fourth', 'Fifth']),
    
    -- Test Case 4: Two messages with special characters
    ('MSG004', ARRAY['Alert: System ready!', 'Notification: Process completed.']),
    
    -- Test Case 5: Empty array (edge case - should produce 0 rows)
    ('MSG005', CAST(NULL ,ARRAY<STRING>)),
    
    -- Test Case 6: Messages with different content types
    ('MSG006', ARRAY['Morning', 'Afternoon', 'Evening', 'Night']);
