with expected_results as (
    select 
    'group_id_1' as expected_group_id,    
    'tenant_id_1' as expected_tenant_id,    
    'group_name_1' as expected_group_name,    
    'group_type_1' as expected_group_type,    
    'tenant_name_1' as expected_tenant_name,    
    'created_date_1' as expected_created_date,    
    'false' as expected_is_active,    
    '2021-01-01 00:00:00' as expected_updated_at    
     
    union all
    select 
    'group_id_2' as expected_group_id,    
    'tenant_id_2' as expected_tenant_id,    
    'group_name_2' as expected_group_name,    
    'group_type_2' as expected_group_type,    
    'tenant_name_2' as expected_tenant_name,    
    'created_date_2' as expected_created_date,    
    'true' as expected_is_active,    
    '2021-01-01 00:00:00' as expected_updated_at    
     
    union all
    select 
    'group_id_3' as expected_group_id,    
    'tenant_id_3' as expected_tenant_id,    
    'group_name_3' as expected_group_name,    
    'group_type_3' as expected_group_type,    
    'tenant_name_3' as expected_tenant_name,    
    'created_date_3' as expected_created_date,    
    'false' as expected_is_active,    
    '2021-01-01 00:00:00' as expected_updated_at    
      
),
actual_results as (
    select 
        group_id,
        tenant_id,
        group_name,
        group_type,
        tenant_name,
        created_date,
        is_active,
        updated_at
    
    from c360_dim_groups_ut
),
validation_check as (
    select 
        e.expected_group_id,
        e.expected_tenant_id,
        e.expected_group_name,
        e.expected_group_type,
        e.expected_tenant_name,
        e.expected_created_date,
        e.expected_is_active,
        e.expected_updated_at,
        
        case when a.group_id = e.expected_group_id then 'PASS' else 'FAIL' end as group_id_check,
        case when a.tenant_id = e.expected_tenant_id then 'PASS' else 'FAIL' end as tenant_id_check,
        case when a.group_name = e.expected_group_name then 'PASS' else 'FAIL' end as group_name_check,
        case when a.group_type = e.expected_group_type then 'PASS' else 'FAIL' end as group_type_check,
        case when a.tenant_name = e.expected_tenant_name then 'PASS' else 'FAIL' end as tenant_name_check,
        case when a.created_date = e.expected_created_date then 'PASS' else 'FAIL' end as created_date_check,
        case when a.is_active = e.expected_is_active then 'PASS' else 'FAIL' end as is_active_check,
        case when a.updated_at = e.expected_updated_at then 'PASS' else 'FAIL' end as updated_at_check
    
    from expected_results e
    join actual_results a
),
overall_result as (
    select 
        count(*) as total_expected_records,
        sum(case when group_id_check = 'PASS' AND tenant_id_check = 'PASS' AND group_name_check = 'PASS' AND group_type_check = 'PASS' AND tenant_name_check = 'PASS' AND created_date_check = 'PASS' AND is_active_check = 'PASS' AND updated_at_check = 'PASS' then 1 else 0 end) as passing_records,
        (select count(*) from actual_results) as actual_record_count
    from validation_check
)
select 
    case 
        when total_expected_records = 3  
         and passing_records = 3
        then 'PASS' 
        else 'FAIL' 
    end as test_result,
    total_expected_records,
    passing_records
from overall_result