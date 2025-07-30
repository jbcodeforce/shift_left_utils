-- Product Analytics with Window Functions and Time-based Aggregations
SELECT 
    product_id,
    category,
    event_timestamp,
    user_id,
    event_type,
    revenue,
    -- Window functions for analytics
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_timestamp DESC) as latest_event_rank,
    LAG(revenue, 1) OVER (PARTITION BY product_id ORDER BY event_timestamp) as prev_revenue,
    SUM(revenue) OVER (PARTITION BY product_id ORDER BY event_timestamp 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_revenue,
    RANK() OVER (PARTITION BY category ORDER BY revenue DESC) as revenue_rank_in_category,
    -- Date/time functions
    DATE_FORMAT(event_timestamp, 'yyyy-MM-dd') as event_date,
    HOUR(event_timestamp) as event_hour,
    DAYOFWEEK(event_timestamp) as day_of_week,
    -- Conditional aggregations
    CASE 
        WHEN event_type = 'purchase' THEN revenue 
        ELSE 0 
    END as purchase_revenue,
    CASE 
        WHEN HOUR(event_timestamp) BETWEEN 9 AND 17 THEN 'business_hours'
        ELSE 'off_hours'
    END as time_category
FROM raw_product_events
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
    AND revenue > 0
ORDER BY product_id, event_timestamp DESC 