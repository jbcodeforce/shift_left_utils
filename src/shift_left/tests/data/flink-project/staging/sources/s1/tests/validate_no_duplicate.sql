-- update with the needed keys
SELECT
  __db,
  COUNT(*) AS cnt
FROM 
TABLE(
    tumble(
      TABLE int_s1_deduped,
      DESCRIPTOR($rowtime),
      INTERVAL '1' hour
    )
  )
GROUP
  BY `__db`
HAVING
  COUNT(*) > 1;