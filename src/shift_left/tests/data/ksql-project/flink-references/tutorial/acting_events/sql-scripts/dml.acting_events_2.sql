INSERT INTO acting_events_other
SELECT name, title, genre
FROM acting_events
WHERE genre != 'drama' AND genre != 'fantasy';