INSERT INTO fct_users
WITH user_groups AS (
    SELECT 
        group_id,
        group_name,
        group_type,
        created_date,
        is_active
    FROM dim_user_groups
    WHERE is_active = true
),
active_users AS (
    SELECT 
        user_id,
        username,
        email,
        group_id,
        last_login_date
    FROM raw_active_users
    WHERE last_login_date >= CURRENT_DATE - INTERVAL 30 DAYS
),
non_active_users AS (
    SELECT 
        user_id,
        username,
        email,
        group_id,
        last_login_date
    FROM raw_active_users
    WHERE last_login_date < CURRENT_DATE - INTERVAL 30 DAYS
),
all_users AS (
    SELECT * FROM active_users
    UNION ALL
    SELECT * FROM non_active_users
),
final_table AS (
    SELECT 
        au.user_id,
        au.username,
        au.email,
        au.last_login_date,
        ug.group_name,
        ug.group_type,
        ug.created_date,
        CASE 
            WHEN au.last_login_date >= CURRENT_DATE - INTERVAL 30 DAYS 
            THEN 'Active'
            ELSE 'Inactive'
        END AS user_status
    FROM all_users au
    LEFT JOIN user_groups ug ON au.group_id = ug.group_id
)
SELECT
    user_id,
    username,
    email,
    group_name,
    group_type,
    user_status,
    last_login_date,
    created_date
FROM final_table
ORDER BY group_name, username;