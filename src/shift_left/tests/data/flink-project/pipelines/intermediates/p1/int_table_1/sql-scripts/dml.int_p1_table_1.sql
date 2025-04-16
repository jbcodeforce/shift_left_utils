INSERT INTO int_p1_table_1
SELECT 
  u.user_id as id, 
  u.user_name, 
  b.account_id, 
  b.account_name, 
  b.balance,
  0 as new_att
FROM src_table_1 u
left join src_p1_table_3 b on u.user_id = b.user_id;
