with result_table as (
   select * from KPI_CONFIG_TABLE_ut
   where id IS NOT NULL
   --- and ... add more validations here
)
SELECT CASE WHEN count(*)=1 THEN 'PASS' ELSE 'FAIL' END as test_result from result_table