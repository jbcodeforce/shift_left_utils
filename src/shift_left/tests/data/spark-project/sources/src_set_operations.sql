-- Set Operations and Complex Subqueries
WITH current_active_users AS (
    SELECT DISTINCT user_id, email, registration_date, last_activity_date
    FROM user_activities
    WHERE last_activity_date >= CURRENT_DATE - INTERVAL 30 DAYS
),
historical_users AS (
    SELECT DISTINCT user_id, email, registration_date, last_activity_date
    FROM user_activities
    WHERE last_activity_date >= CURRENT_DATE - INTERVAL 90 DAYS
        AND last_activity_date < CURRENT_DATE - INTERVAL 30 DAYS
),
new_users_this_month AS (
    SELECT user_id, email, registration_date
    FROM current_active_users
    WHERE registration_date >= CURRENT_DATE - INTERVAL 30 DAYS
),
churned_users AS (
    -- Users who were active 30-90 days ago but not in last 30 days
    SELECT user_id, email, registration_date, last_activity_date
    FROM historical_users
    EXCEPT
    SELECT user_id, email, registration_date, last_activity_date
    FROM current_active_users
),
retained_users AS (
    -- Users active both in current and historical periods
    SELECT h.user_id, h.email, h.registration_date
    FROM historical_users h
    INNER JOIN current_active_users c ON h.user_id = c.user_id
),
reactivated_users AS (
    -- Users who were inactive but became active again
    SELECT c.user_id, c.email, c.registration_date
    FROM current_active_users c
    WHERE c.user_id IN (
        SELECT user_id 
        FROM user_activities 
        WHERE last_activity_date < CURRENT_DATE - INTERVAL 60 DAYS
        AND user_id NOT IN (
            SELECT user_id 
            FROM user_activities 
            WHERE last_activity_date BETWEEN CURRENT_DATE - INTERVAL 60 DAYS 
                                          AND CURRENT_DATE - INTERVAL 30 DAYS
        )
    )
),
user_segments AS (
    SELECT 
        'new_users' as segment_type,
        COUNT(*) as user_count,
        AVG(DATEDIFF(CURRENT_DATE, registration_date)) as avg_days_since_registration
    FROM new_users_this_month
    
    UNION ALL
    
    SELECT 
        'churned_users' as segment_type,
        COUNT(*) as user_count,
        AVG(DATEDIFF(CURRENT_DATE, registration_date)) as avg_days_since_registration
    FROM churned_users
    
    UNION ALL
    
    SELECT 
        'retained_users' as segment_type,
        COUNT(*) as user_count,
        AVG(DATEDIFF(CURRENT_DATE, registration_date)) as avg_days_since_registration
    FROM retained_users
    
    UNION ALL
    
    SELECT 
        'reactivated_users' as segment_type,
        COUNT(*) as user_count,
        AVG(DATEDIFF(CURRENT_DATE, registration_date)) as avg_days_since_registration
    FROM reactivated_users
),
cohort_analysis AS (
    SELECT 
        DATE_FORMAT(registration_date, 'yyyy-MM') as registration_month,
        COUNT(DISTINCT CASE WHEN user_id IN (SELECT user_id FROM current_active_users) 
                           THEN user_id END) as active_users,
        COUNT(DISTINCT user_id) as total_registered_users,
        ROUND(
            (COUNT(DISTINCT CASE WHEN user_id IN (SELECT user_id FROM current_active_users) 
                                THEN user_id END) * 100.0) / COUNT(DISTINCT user_id), 2
        ) as retention_rate
    FROM user_activities
    WHERE registration_date >= CURRENT_DATE - INTERVAL 12 MONTHS
    GROUP BY DATE_FORMAT(registration_date, 'yyyy-MM')
),
feature_adoption AS (
    SELECT 
        f.feature_name,
        COUNT(DISTINCT ua.user_id) as adopters,
        COUNT(DISTINCT cau.user_id) as total_active_users,
        ROUND(
            (COUNT(DISTINCT ua.user_id) * 100.0) / COUNT(DISTINCT cau.user_id), 2
        ) as adoption_rate
    FROM feature_usage f
    CROSS JOIN current_active_users cau
    LEFT JOIN user_activities ua ON f.user_id = ua.user_id 
                                 AND f.feature_name = ua.feature_used
                                 AND ua.last_activity_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY f.feature_name
)
-- Final result combining all analyses
SELECT 
    segment_type,
    user_count,
    ROUND(avg_days_since_registration, 1) as avg_days_since_registration,
    ROUND((user_count * 100.0) / SUM(user_count) OVER (), 2) as percentage_of_total
FROM user_segments

UNION ALL

SELECT 
    CONCAT('cohort_', registration_month) as segment_type,
    active_users as user_count,
    NULL as avg_days_since_registration,
    retention_rate as percentage_of_total
FROM cohort_analysis
WHERE retention_rate IS NOT NULL

ORDER BY segment_type 