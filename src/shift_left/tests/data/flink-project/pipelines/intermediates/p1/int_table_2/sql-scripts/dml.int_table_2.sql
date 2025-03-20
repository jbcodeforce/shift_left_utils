INSERT INTO int_table_2
  product_id,
  product
SELECT 
-- part to select stuff
FROM src_table_2
WHERE product <> NULL;