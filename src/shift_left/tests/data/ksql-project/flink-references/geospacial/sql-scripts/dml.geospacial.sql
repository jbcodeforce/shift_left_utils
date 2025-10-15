INSERT INTO destinations
SELECT 
    engineer.engineer_id as engineer_id,
    ticket.longitude,
    ticket.latitude
FROM engineers AS engineer
JOIN tickets AS ticket
  FOR SYSTEM_TIME AS OF engineer.$rowtime
  ON GEO_DISTANCE(engineer.latitude, engineer.longitude, ticket.latitude, ticket.longitude) <= 500;