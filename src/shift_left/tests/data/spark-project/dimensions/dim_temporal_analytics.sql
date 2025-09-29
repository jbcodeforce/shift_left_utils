-- Complex Temporal Analytics and Time Series Analysis
WITH time_series_base AS (
    SELECT 
        user_id,
        event_timestamp,
        event_type,
        revenue_amount,
        -- Time extractions
        DATE(event_timestamp) as event_date,
        HOUR(event_timestamp) as event_hour,
        DAYOFWEEK(event_timestamp) as day_of_week,
        WEEKOFYEAR(event_timestamp) as week_of_year,
        MONTH(event_timestamp) as event_month,
        QUARTER(event_timestamp) as event_quarter,
        -- Time-based groupings
        DATE_TRUNC('hour', event_timestamp) as hour_bucket,
        DATE_TRUNC('day', event_timestamp) as day_bucket,
        DATE_TRUNC('week', event_timestamp) as week_bucket,
        DATE_TRUNC('month', event_timestamp) as month_bucket,
        -- Business time calculations
        CASE 
            WHEN DAYOFWEEK(event_timestamp) IN (1, 7) THEN 'weekend'
            WHEN HOUR(event_timestamp) BETWEEN 9 AND 17 THEN 'business_hours'
            ELSE 'after_hours'
        END as time_category,
        -- Seasonal indicators
        CASE 
            WHEN MONTH(event_timestamp) IN (12, 1, 2) THEN 'winter'
            WHEN MONTH(event_timestamp) IN (3, 4, 5) THEN 'spring'
            WHEN MONTH(event_timestamp) IN (6, 7, 8) THEN 'summer'
            ELSE 'fall'
        END as season
    FROM user_events
    WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 90 DAYS
),
hourly_patterns AS (
    SELECT 
        event_hour,
        day_of_week,
        time_category,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        SUM(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE 0 END) as hourly_revenue,
        AVG(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE NULL END) as avg_purchase_amount,
        -- Calculate hourly conversion rates
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        CASE 
            WHEN SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) > 0
            THEN (SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0) / 
                 SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)
            ELSE 0
        END as conversion_rate,
        -- Time-based rankings
        RANK() OVER (PARTITION BY day_of_week ORDER BY COUNT(*) DESC) as hourly_rank_by_day,
        PERCENT_RANK() OVER (ORDER BY COUNT(*)) as activity_percentile
    FROM time_series_base
    GROUP BY event_hour, day_of_week, time_category
),
daily_trends AS (
    SELECT 
        event_date,
        day_of_week,
        season,
        COUNT(*) as daily_events,
        COUNT(DISTINCT user_id) as daily_active_users,
        SUM(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE 0 END) as daily_revenue,
        -- Rolling averages
        AVG(COUNT(*)) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_avg_events,
        AVG(SUM(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE 0 END)) 
            OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_avg_revenue,
        -- Period over period comparisons
        LAG(COUNT(*), 1) OVER (ORDER BY event_date) as prev_day_events,
        LAG(COUNT(*), 7) OVER (ORDER BY event_date) as same_day_last_week_events,
        LAG(SUM(CASE WHEN event_type = 'purchase' THEN revenue_amount ELSE 0 END), 1) 
            OVER (ORDER BY event_date) as prev_day_revenue,
        -- Growth calculations
        CASE 
            WHEN LAG(COUNT(*), 1) OVER (ORDER BY event_date) > 0
            THEN ((COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY event_date)) * 100.0) / 
                 LAG(COUNT(*), 1) OVER (ORDER BY event_date)
            ELSE NULL
        END as day_over_day_growth,
        CASE 
            WHEN LAG(COUNT(*), 7) OVER (ORDER BY event_date) > 0
            THEN ((COUNT(*) - LAG(COUNT(*), 7) OVER (ORDER BY event_date)) * 100.0) / 
                 LAG(COUNT(*), 7) OVER (ORDER BY event_date)
            ELSE NULL
        END as week_over_week_growth
    FROM time_series_base
    GROUP BY event_date, day_of_week, season
),
cohort_retention AS (
    SELECT 
        DATE_TRUNC('month', first_event_date) as cohort_month,
        DATEDIFF(DATE_TRUNC('month', event_timestamp), 
                DATE_TRUNC('month', first_event_date)) / 30 as period_number,
        COUNT(DISTINCT user_id) as active_users,
        -- Calculate retention rates
        FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (
            PARTITION BY DATE_TRUNC('month', first_event_date) 
            ORDER BY DATEDIFF(DATE_TRUNC('month', event_timestamp), 
                             DATE_TRUNC('month', first_event_date)) / 30
        ) as cohort_size,
        ROUND(
            (COUNT(DISTINCT user_id) * 100.0) / 
            FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (
                PARTITION BY DATE_TRUNC('month', first_event_date) 
                ORDER BY DATEDIFF(DATE_TRUNC('month', event_timestamp), 
                                 DATE_TRUNC('month', first_event_date)) / 30
            ), 2
        ) as retention_rate
    FROM (
        SELECT 
            user_id,
            event_timestamp,
            MIN(event_timestamp) OVER (PARTITION BY user_id) as first_event_date
        FROM time_series_base
    ) cohort_data
    GROUP BY 
        DATE_TRUNC('month', first_event_date),
        DATEDIFF(DATE_TRUNC('month', event_timestamp), 
                DATE_TRUNC('month', first_event_date)) / 30
),
seasonality_analysis AS (
    SELECT 
        season,
        event_month,
        AVG(daily_events) as avg_daily_events,
        AVG(daily_revenue) as avg_daily_revenue,
        STDDEV(daily_events) as stddev_daily_events,
        STDDEV(daily_revenue) as stddev_daily_revenue,
        -- Coefficient of variation
        CASE 
            WHEN AVG(daily_events) > 0 
            THEN (STDDEV(daily_events) / AVG(daily_events)) * 100
            ELSE NULL
        END as events_coefficient_of_variation,
        -- Seasonal indices
        AVG(daily_events) / (
            SELECT AVG(daily_events) FROM daily_trends
        ) as seasonal_index_events,
        AVG(daily_revenue) / (
            SELECT AVG(daily_revenue) FROM daily_trends WHERE daily_revenue > 0
        ) as seasonal_index_revenue
    FROM daily_trends
    GROUP BY season, event_month
)
-- Final comprehensive temporal analysis
SELECT 
    'hourly_peak_analysis' as analysis_type,
    CAST(event_hour AS STRING) as time_dimension,
    CAST(MAX(event_count) AS STRING) as metric_value,
    'Peak hour activity' as description
FROM hourly_patterns
WHERE day_of_week BETWEEN 2 AND 6  -- Weekdays only
GROUP BY event_hour
HAVING MAX(event_count) = (
    SELECT MAX(event_count) 
    FROM hourly_patterns 
    WHERE day_of_week BETWEEN 2 AND 6
)

UNION ALL

SELECT 
    'seasonality_analysis' as analysis_type,
    season as time_dimension,
    CAST(ROUND(AVG(seasonal_index_events), 3) AS STRING) as metric_value,
    'Seasonal activity index' as description
FROM seasonality_analysis
GROUP BY season

UNION ALL

SELECT 
    'growth_trends' as analysis_type,
    'week_over_week' as time_dimension,
    CAST(ROUND(AVG(week_over_week_growth), 2) AS STRING) as metric_value,
    'Average weekly growth rate' as description
FROM daily_trends
WHERE week_over_week_growth IS NOT NULL

ORDER BY analysis_type, time_dimension 