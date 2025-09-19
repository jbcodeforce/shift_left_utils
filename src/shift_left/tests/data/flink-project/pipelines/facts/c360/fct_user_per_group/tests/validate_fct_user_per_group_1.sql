with expected_results as (
    select 
     -- change the value of the expected_ to the correct value
        'group_id' as expected_group_id,    
       'group_name' as expected_group_name,    
       'group_type' as expected_group_type,    
       'total_users' as expected_total_users,    
       'active_users' as expected_active_users,    
       'inactive_users' as expected_inactive_users,    
       'latest_user_created_date' as expected_latest_user_created_date,    
       'fact_updated_at' as expected_fact_updated_at,    
       
    -- potential add other values here
    -- union all
    
),
actual_results as (
    select 
        group_id,
        group_name,
        group_type,
        total_users,
        active_users,
        inactive_users,
        latest_user_created_date,
        fact_updated_at,
        
    from fct_user_per_group_ut
),
validation_check as (
    select 
       
        e.expected_group_id,
        e.expected_group_name,
        e.expected_group_type,
        e.expected_total_users,
        e.expected_active_users,
        e.expected_inactive_users,
        e.expected_latest_user_created_date,
        e.expected_fact_updated_at,
        
        -- be sure to use the correct conditions for the check
        case when a.group_id = e.expected_group_id then 'PASS' else 'FAIL' end as group_id_check,
        case when a.group_name = e.expected_group_name then 'PASS' else 'FAIL' end as group_name_check,
        case when a.group_type = e.expected_group_type then 'PASS' else 'FAIL' end as group_type_check,
        case when a.total_users = e.expected_total_users then 'PASS' else 'FAIL' end as total_users_check,
        case when a.active_users = e.expected_active_users then 'PASS' else 'FAIL' end as active_users_check,
        case when a.inactive_users = e.expected_inactive_users then 'PASS' else 'FAIL' end as inactive_users_check,
        case when a.latest_user_created_date = e.expected_latest_user_created_date then 'PASS' else 'FAIL' end as latest_user_created_date_check,
        case when a.fact_updated_at = e.expected_fact_updated_at then 'PASS' else 'FAIL' end as fact_updated_at_check,
        

    from expected_results e
    left join actual_results a on e.expected_sid = a.sid
),
overall_result as (
    select 
        count(*) as total_expected_records,
        sum(case when group_id_check = 'PASS' AND group_name_check = 'PASS' AND group_type_check = 'PASS' AND total_users_check = 'PASS' AND active_users_check = 'PASS' AND inactive_users_check = 'PASS' AND latest_user_created_date_check = 'PASS' AND fact_updated_at_check = 'PASS' then 1 else 0 end) as passing_records,
        (select count(*) from actual_results) as actual_record_count
    from validation_check
)
select 
    case 
        when total_expected_records = 1  -- should match the number of union
         and passing_records = 1
        then 'PASS' 
        else 'FAIL' 
    end as test_result,
    total_expected_records,
    passing_records
from overall_result