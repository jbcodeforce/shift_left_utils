INSERT INTO fct_user_per_group
with append_users as (
  select 
    user_id,
    group_id,
    group_name,
    group_type,
    is_active,
    LAST_VALUE(created_date) as last_update_date,
    tumble_end(`$rowtime`, interval '10' second) as window_end_time
  from c360_dim_users
  group by
    user_id, group_id, group_name, group_type, is_active,
    tumble_end(`$rowtime`, interval '10' second) 
)
SELECT 
  d.group_id,
  d.group_name,
  d.group_type,
  COUNT(*) as total_users,
  SUM(CASE WHEN d.is_active = true THEN 1 ELSE 0 END) as active_users,
  SUM(CASE WHEN d.is_active = false THEN 1 ELSE 0 END) as inactive_users,
  MAX(CAST(last_update_date as BIGINT)) as latest_user_created_date,
  CAST (NULL as TIMESTAMP) as fact_updated_at
FROM append_users d
GROUP BY 
  d.group_id,
  d.group_name,
  d.group_type