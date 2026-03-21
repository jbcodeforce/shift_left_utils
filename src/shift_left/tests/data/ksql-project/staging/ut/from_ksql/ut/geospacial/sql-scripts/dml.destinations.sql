INSERT INTO destinations
SELECT
    engineer.engineer_id,
    ticket.longitude,
    ticket.latitude,
    window_start,
    window_end
FROM (
    SELECT
        engineer.engineer_id,
        ticket.longitude,
        ticket.latitude,
        TUMBLE_START($rowtime, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END($rowtime, INTERVAL '5' MINUTE) AS window_end
    FROM engineers AS engineer
    JOIN tickets AS ticket
    ON GEO_DISTANCE(engineer.latitude, engineer.longitude, ticket.latitude, ticket.longitude) <= 500
    AND engineer.$rowtime BETWEEN ticket.$rowtime - INTERVAL '5' MINUTE AND ticket.$rowtime
) AS joined_data;