INSERT INTO users_dim_users
SELECT 
  u.user_id,
  u.user_name,
  u.user_email,
  u.group_id,
  g.group_name,
  g.group_type,
  u.created_date,
  u.is_active
FROM src_users_users u
LEFT JOIN src_users_groups g
  ON u.group_id = g.group_id
WHERE u.user_id IS NOT NULL 
  AND u.user_email IS NOT NULL