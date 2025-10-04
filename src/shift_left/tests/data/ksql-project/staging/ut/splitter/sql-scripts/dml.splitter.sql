INSERT INTO single_message_stream
SELECT 
    message
FROM (
    SELECT MESSAGE, UNNEST(MESSAGES) AS message
    FROM multi_message_stream
) AS t;