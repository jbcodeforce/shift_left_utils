with expected_results as (
    select 
    'default_key' as expected_default_key,    
    'a_value' as expected_a_value,    
    'x_value' as expected_x_value    
    
        
    -- union all -- add more union here for each potential test data
    
),
actual_results as (
    select 
        default_key,
        a_value,
        x_value
        
    from a_ut
),
validation_check as (
    select 
       
        e.expected_default_key,
        e.expected_a_value,
        e.expected_x_value,
        
        -- be sure to use the correct conditions for the check
        case when a.default_key = e.expected_default_key then 'PASS' else 'FAIL' end as default_key_check,
        case when a.a_value = e.expected_a_value then 'PASS' else 'FAIL' end as a_value_check,
        case when a.x_value = e.expected_x_value then 'PASS' else 'FAIL' end as x_value_check
        

    from expected_results e
    left join actual_results a on a.sid = e.sid -- !!! change the condition here
),
overall_result as (
    select 
        count(*) as total_expected_records,
        sum(case when default_key_check = 'PASS' AND a_value_check = 'PASS' AND x_value_check = 'PASS' then 1 else 0 end) as passing_records,
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