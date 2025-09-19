INSERT INTO src_c360_users (
  user_id,
  user_name, 
  user_email,
  group_id,
  tenant_id,
  created_date,
  is_active,
  updated_at
)
WITH deduplicated_users AS (
  SELECT 
    user_id,
    user_name,
    user_email,
    group_id,
    tenant_id,
    created_date,
    is_active,
    CURRENT_TIMESTAMP AS updated_at,
    
    -- Deduplication: Keep latest record per user_id
    -- This handles cases where the same user appears multiple times
    ROW_NUMBER() OVER (
      PARTITION BY user_id 
      ORDER BY `$rowtime` DESC
    ) AS row_num
    
  FROM raw_users
  WHERE 
    user_id IS NOT NULL  -- Ensure we have valid user_id
    AND user_email IS NOT NULL  -- Ensure we have valid email
    AND is_active
)
SELECT 
  user_id,
  user_name,
  user_email, 
  group_id, 
  tenant_id,
  created_date,
  is_active,
  updated_at
FROM deduplicated_users
WHERE row_num = 1  -- Keep only the most recent record per user