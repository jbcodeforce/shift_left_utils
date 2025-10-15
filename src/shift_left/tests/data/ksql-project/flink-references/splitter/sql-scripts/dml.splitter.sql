INSERT INTO single_message_stream
SELECT
    message_id,
    message
FROM multi_message_stream
cross JOIN UNNEST(messages) AS t(message);