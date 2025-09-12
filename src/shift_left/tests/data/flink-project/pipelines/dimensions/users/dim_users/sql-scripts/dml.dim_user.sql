INSERT INTO dim_users
-- add CTE to validae the unit tests will not change the name of the CTE
with valid_users as (
  SELECT * FROM src_users_users
  WHERE user_id IS NOT NULL
  AND user_email IS NOT NULL
)
SELECT 
  u.user_id,
  u.user_name,
  u.user_email,
  u.group_id,
  g.group_name,
  g.group_type,
  u.created_date,
  u.is_active
FROM valid_users u
LEFT JOIN src_users_groups g
  ON u.group_id = g.group_id
