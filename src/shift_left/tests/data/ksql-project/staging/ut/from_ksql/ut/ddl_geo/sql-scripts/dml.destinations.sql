INSERT INTO destinations
SELECT engineer.engineer_id, ticket.longitude, ticket.latitude
FROM engineers AS engineer
JOIN tickets AS ticket
ON GEO_DISTANCE(engineer.latitude, engineer.longitude, ticket.latitude, ticket.longitude) <= 500
WHERE engineer.$rowtime BETWEEN ticket.$rowtime - INTERVAL '5' MINUTE AND ticket.$rowtime;