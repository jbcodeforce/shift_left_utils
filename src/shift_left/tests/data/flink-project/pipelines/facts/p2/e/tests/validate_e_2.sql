with result_table as (
   select * from e_ut
   where id != NULL
   --- and ... add more validations here
)
SELECT CASE WHEN count(*)=1 THEN 'PASS' ELSE 'FAIL' END as result from result_table