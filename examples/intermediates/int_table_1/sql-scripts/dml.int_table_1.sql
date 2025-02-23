INSERT INTO int_table_1
SELECT 
-- part to select stuff
FROM src_table_1 a
join src_table_3 b on a.id = b.id 
WHERE -- where condition or remove it