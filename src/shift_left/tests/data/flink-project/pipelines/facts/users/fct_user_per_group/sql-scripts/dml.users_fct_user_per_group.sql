INSERT INTO users_fct_user_per_group
SELECT 
  d.group_id,
  d.group_name,
  d.group_type,
  COUNT(*) as total_users,
  SUM(CASE WHEN d.is_active = true THEN 1 ELSE 0 END) as active_users,
  SUM(CASE WHEN d.is_active = false THEN 1 ELSE 0 END) as inactive_users,
  MAX(d.created_date) as latest_user_created_date,
  CURRENT_TIMESTAMP as fact_updated_at
FROM users_dim_users d
WHERE d.group_id IS NOT NULL 
  AND d.group_name IS NOT NULL
GROUP BY 
  d.group_id, 
  d.group_name, 
  d.group_type