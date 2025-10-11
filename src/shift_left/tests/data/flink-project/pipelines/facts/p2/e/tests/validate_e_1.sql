with expected_results as (
    select 
    'default_key' as expected_default_key,    
    'e_field' as expected_e_field,    
    'c_value' as expected_c_value    
    
        
    -- union all -- add more union here for each potential test data
    
),
actual_results as (
    select 
        default_key,
        e_field,
        c_value
        
    from e_ut
),
validation_check as (
    select 
       
        e.expected_default_key,
        e.expected_e_field,
        e.expected_c_value,
        
        -- be sure to use the correct conditions for the check
        case when a.default_key = e.expected_default_key then 'PASS' else 'FAIL' end as default_key_check,
        case when a.e_field = e.expected_e_field then 'PASS' else 'FAIL' end as e_field_check,
        case when a.c_value = e.expected_c_value then 'PASS' else 'FAIL' end as c_value_check
        

    from expected_results e
    left join actual_results a on a.sid = e.sid -- !!! change the condition here
),
overall_result as (
    select 
        count(*) as total_expected_records,
        sum(case when default_key_check = 'PASS' AND e_field_check = 'PASS' AND c_value_check = 'PASS' then 1 else 0 end) as passing_records,
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