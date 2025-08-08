-- Customer Journey Analysis with Complex CTEs and Joins
WITH customer_sessions AS (
    SELECT 
        customer_id,
        session_id,
        MIN(event_timestamp) as session_start,
        MAX(event_timestamp) as session_end,
        COUNT(*) as total_events,
        COUNT(DISTINCT page_url) as unique_pages_visited,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_in_session
    FROM web_events
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY customer_id, session_id
),
customer_segments AS (
    SELECT 
        customer_id,
        age_group,
        location,
        membership_tier,
        registration_date,
        CASE 
            WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 30 THEN 'new'
            WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 365 THEN 'regular'
            ELSE 'veteran'
        END as customer_segment
    FROM customer_profiles
),
purchase_history AS (
    SELECT 
        customer_id,
        COUNT(*) as total_purchases,
        SUM(amount) as total_spent,
        AVG(amount) as avg_purchase_amount,
        MAX(purchase_date) as last_purchase_date,
        MIN(purchase_date) as first_purchase_date
    FROM purchases
    WHERE purchase_date >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY customer_id
),

customer_metrics AS (
    SELECT 
        cs.customer_id,
        seg.customer_segment,
        seg.membership_tier,
        seg.location,
        COUNT(DISTINCT cs.session_id) as total_sessions,
        AVG(cs.total_events) as avg_events_per_session,
        SUM(cs.purchases_in_session) as total_web_purchases,
        COALESCE(ph.total_purchases, 0) as total_purchases,
        COALESCE(ph.total_spent, 0) as total_spent,
        COALESCE(ph.avg_purchase_amount, 0) as avg_purchase_amount,
        DATEDIFF(CURRENT_DATE, ph.last_purchase_date) as days_since_last_purchase
    FROM customer_sessions cs
    LEFT JOIN customer_segments seg ON cs.customer_id = seg.customer_id
    LEFT JOIN purchase_history ph ON cs.customer_id = ph.customer_id
    GROUP BY cs.customer_id, seg.customer_segment, seg.membership_tier, 
             seg.location, ph.total_purchases, ph.total_spent, 
             ph.avg_purchase_amount, ph.last_purchase_date
)
SELECT 
    customer_segment,
    membership_tier,
    location,
    COUNT(*) as customer_count,
    AVG(total_sessions) as avg_sessions,
    AVG(avg_events_per_session) as avg_events_per_session,
    AVG(total_spent) as avg_total_spent,
    PERCENTILE_APPROX(total_spent, 0.5) as median_total_spent,
    SUM(CASE WHEN days_since_last_purchase <= 7 THEN 1 ELSE 0 END) as recent_purchasers,
    SUM(CASE WHEN days_since_last_purchase > 30 THEN 1 ELSE 0 END) as at_risk_customers
FROM customer_metrics
GROUP BY customer_segment, membership_tier, location
ORDER BY customer_segment, membership_tier, location 


