SELECT CASE WHEN count(*)=6 THEN 'PASS' ELSE 'FAIL' END from
  ( SELECT * FROM
  src_table_2 a
  INNER JOIN
  tgt_table_2 b
  on a.order_id=b.order_id) c;