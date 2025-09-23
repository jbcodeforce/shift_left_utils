-- Integration test validation for fct_user_per_group
-- This query should validate that the expected data reached the sink table

WITH validation_result AS (
    SELECT 
        COUNT(*) as record_count,
        -- Add specific validations based on your business logic
        MIN(test_timestamp) as min_timestamp,
        MAX(test_timestamp) as max_timestamp
    FROM fct_user_per_group_it
    WHERE test_unique_id = '{test_unique_id}'  -- Will be replaced at runtime
)
SELECT 
    CASE 
        WHEN record_count > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result,
    record_count,
    min_timestamp,
    max_timestamp
FROM validation_result;
