INSERT INTO int_table_1
SELECT 
  u.user_id as id, u.user_name, b.account_id, b.account_name, b.balance
FROM src_table_1 u
left join src_table_3 b on u.user_id = b.user_id;
