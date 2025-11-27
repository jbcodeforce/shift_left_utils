INSERT INTO single_message_stream
SELECT u.element as message
FROM multi_message_stream
CROSS JOIN UNNEST(messages) AS u(element);