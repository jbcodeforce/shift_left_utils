INSERT INTO int_table_1
SELECT 
  id, user_name, account
FROM src_table_1 a
left join src_table_3 b on a.id = b.id;
