INSERT INTO src_c360_groups (
  group_id,
  tenant_id,
  group_name,
  group_type,
  created_date,
  is_active,
  updated_at
)
WITH deduplicated_groups AS (
  SELECT 
    group_id,
    tenant_id,
    group_name,
    group_type,
    created_date,
    is_active,
    CURRENT_TIMESTAMP AS updated_at,
    
    -- Deduplication: Keep latest record per group_id
    -- This handles cases where the same group appears multiple times
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id, group_id 
      ORDER BY `$rowtime` DESC
    ) AS row_num
    
  FROM raw_groups
  WHERE 
    group_id IS NOT NULL  -- Ensure we have valid group_id  and tenant_id
    AND tenant_id IS NOT NULL  
)
SELECT 
  group_id,
  tenant_id,
  group_name,
  group_type,
  created_date,
  is_active,
  updated_at
FROM deduplicated_groups
WHERE row_num = 1  -- Keep only the most recent record per group