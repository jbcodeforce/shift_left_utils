INSERT INTO acting_events_drama SELECT name, title FROM acting_events WHERE genre='drama';
INSERT INTO acting_events_fantasy SELECT name, title FROM acting_events WHERE genre='fantasy';
INSERT INTO acting_events_other SELECT name, title, genre FROM acting_events WHERE genre != 'drama' AND genre != 'fantasy';