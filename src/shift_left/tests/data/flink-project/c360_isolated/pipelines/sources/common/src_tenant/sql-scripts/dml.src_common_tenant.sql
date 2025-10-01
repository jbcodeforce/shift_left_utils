INSERT INTO src_common_tenant
with extracted_data as (
SELECT 
  coalesce(if(op = 'd', before.tenant_id, after.tenant_id), 'dummy_tenant_id') as tenant_id,
  coalesce(if(op = 'd', before.tenant_name, after.tenant_name), 'dummy_tenant_name') as tenant_name,
  coalesce(if(op = 'd', before.tenant_description, after.tenant_description), 'dummy_tenant_description') as tenant_description,
  coalesce(if(op = 'd', before.tenant_status, after.tenant_status), 'dummy_tenant_status') as tenant_status,
  to_timestamp(coalesce(if(op = 'd', before.tenant_created_at, after.tenant_created_at), '2025-09-10T12:00:00.000'), 'yyyy-MM-dd''T''HH:mm:ss.SSS') as tenant_created_at,
  to_timestamp(coalesce(if(op = 'd', before.tenant_updated_at, after.tenant_updated_at), '2025-09-10T12:00:00.000'), 'yyyy-MM-dd''T''HH:mm:ss.SSS') as tenant_updated_at,
  op,
  ts_ms
FROM raw_tenants where not (before is null and after is null)
),
final as (select * 
        FROM  (select *,  ROW_NUMBER() OVER (
                PARTITION BY tenant_id
                ORDER
                    BY ts_ms DESC
                ) AS row_num from extracted_data
        ) where row_num = 1
)
SELECT 
    tenant_id,
    tenant_name,
    tenant_description,
    tenant_status,
    tenant_created_at,
    tenant_updated_at
FROM final;
