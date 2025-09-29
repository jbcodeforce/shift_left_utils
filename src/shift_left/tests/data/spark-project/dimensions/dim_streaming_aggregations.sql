-- Streaming Aggregations with Time Windows and Real-time Analytics
SELECT 
    -- Time window aggregations
    WINDOW(event_timestamp, '5 minutes') as time_window,
    user_segment,
    event_type,
    source_system,
    -- Basic aggregations
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions,
    -- Revenue aggregations
    SUM(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE 0 END) as total_revenue,
    AVG(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE NULL END) as avg_purchase_amount,
    MAX(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE NULL END) as max_purchase_amount,
    -- Conversion metrics
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as add_to_cart_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_events,
    -- Calculate conversion rates
    CASE 
        WHEN SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) * 100.0) / 
             SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)
        ELSE 0
    END as page_to_cart_conversion_rate,
    CASE 
        WHEN SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0) / 
             SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)
        ELSE 0
    END as cart_to_purchase_conversion_rate,
    -- Error rate monitoring
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count,
    CASE 
        WHEN COUNT(*) > 0
        THEN (SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0
    END as error_rate_percent,
    -- Response time metrics
    AVG(response_time_ms) as avg_response_time,
    PERCENTILE_APPROX(response_time_ms, 0.5) as median_response_time,
    PERCENTILE_APPROX(response_time_ms, 0.95) as p95_response_time,
    PERCENTILE_APPROX(response_time_ms, 0.99) as p99_response_time,
    -- Geographic distribution
    COUNT(DISTINCT geo_country) as unique_countries,
    COUNT(DISTINCT geo_city) as unique_cities,
    -- Device analytics
    SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) as mobile_events,
    SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) as desktop_events,
    SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) as tablet_events,
    -- Traffic source analysis
    COUNT(DISTINCT traffic_source) as unique_traffic_sources,
    SUM(CASE WHEN traffic_source = 'organic' THEN 1 ELSE 0 END) as organic_traffic,
    SUM(CASE WHEN traffic_source = 'paid' THEN 1 ELSE 0 END) as paid_traffic,
    SUM(CASE WHEN traffic_source = 'social' THEN 1 ELSE 0 END) as social_traffic,
    -- Real-time anomaly detection indicators
    CASE 
        WHEN COUNT(*) > (AVG(COUNT(*)) OVER (ORDER BY WINDOW(event_timestamp, '5 minutes') 
                                           ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING)) * 2
        THEN 'HIGH_TRAFFIC_SPIKE'
        WHEN COUNT(*) < (AVG(COUNT(*)) OVER (ORDER BY WINDOW(event_timestamp, '5 minutes') 
                                           ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING)) * 0.5
        THEN 'LOW_TRAFFIC_DROP'
        ELSE 'NORMAL'
    END as traffic_anomaly_indicator,
    -- Seasonal patterns
    HOUR(WINDOW(event_timestamp, '5 minutes').start) as window_hour,
    DAYOFWEEK(WINDOW(event_timestamp, '5 minutes').start) as window_day_of_week
FROM streaming_events
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
GROUP BY 
    WINDOW(event_timestamp, '5 minutes'),
    user_segment,
    event_type,
    source_system
ORDER BY time_window DESC, user_segment, event_type 