-- Sales Analysis with Pivot Operations and Advanced Analytics
WITH monthly_sales AS (
    SELECT 
        product_category,
        region,
        DATE_FORMAT(sale_date, 'yyyy-MM') as sale_month,
        SUM(sale_amount) as monthly_revenue,
        COUNT(*) as monthly_transactions,
        AVG(sale_amount) as avg_transaction_value
    FROM sales_data
    WHERE sale_date >= ADD_MONTHS(CURRENT_DATE, -12)
    GROUP BY product_category, region, DATE_FORMAT(sale_date, 'yyyy-MM')
),
pivoted_sales AS (
    SELECT 
        product_category,
        region,
        -- Pivot monthly sales into columns
        SUM(CASE WHEN sale_month = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -1), 'yyyy-MM') 
                 THEN monthly_revenue ELSE 0 END) as last_month_revenue,
        SUM(CASE WHEN sale_month = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -2), 'yyyy-MM') 
                 THEN monthly_revenue ELSE 0 END) as two_months_ago_revenue,
        SUM(CASE WHEN sale_month = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -3), 'yyyy-MM') 
                 THEN monthly_revenue ELSE 0 END) as three_months_ago_revenue,
        -- Pivot transaction counts
        SUM(CASE WHEN sale_month = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -1), 'yyyy-MM') 
                 THEN monthly_transactions ELSE 0 END) as last_month_transactions,
        SUM(CASE WHEN sale_month = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -2), 'yyyy-MM') 
                 THEN monthly_transactions ELSE 0 END) as two_months_ago_transactions,
        -- Quarter over quarter metrics
        SUM(CASE WHEN sale_month IN (
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -1), 'yyyy-MM'),
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -2), 'yyyy-MM'),
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -3), 'yyyy-MM')
            ) THEN monthly_revenue ELSE 0 END) as current_quarter_revenue,
        SUM(CASE WHEN sale_month IN (
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -4), 'yyyy-MM'),
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -5), 'yyyy-MM'),
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -6), 'yyyy-MM')
            ) THEN monthly_revenue ELSE 0 END) as previous_quarter_revenue,
        -- Year over year comparison
        SUM(CASE WHEN sale_month = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -12), 'yyyy-MM') 
                 THEN monthly_revenue ELSE 0 END) as same_month_last_year_revenue
    FROM monthly_sales
    GROUP BY product_category, region
),
analytics_metrics AS (
    SELECT 
        product_category,
        region,
        last_month_revenue,
        two_months_ago_revenue,
        three_months_ago_revenue,
        current_quarter_revenue,
        previous_quarter_revenue,
        same_month_last_year_revenue,
        -- Growth calculations
        CASE 
            WHEN two_months_ago_revenue > 0 
            THEN ((last_month_revenue - two_months_ago_revenue) / two_months_ago_revenue) * 100
            ELSE NULL
        END as mom_growth_percent,
        CASE 
            WHEN previous_quarter_revenue > 0 
            THEN ((current_quarter_revenue - previous_quarter_revenue) / previous_quarter_revenue) * 100
            ELSE NULL
        END as qoq_growth_percent,
        CASE 
            WHEN same_month_last_year_revenue > 0 
            THEN ((last_month_revenue - same_month_last_year_revenue) / same_month_last_year_revenue) * 100
            ELSE NULL
        END as yoy_growth_percent,
        -- Trend analysis
        CASE 
            WHEN last_month_revenue > two_months_ago_revenue 
                 AND two_months_ago_revenue > three_months_ago_revenue 
            THEN 'upward_trend'
            WHEN last_month_revenue < two_months_ago_revenue 
                 AND two_months_ago_revenue < three_months_ago_revenue 
            THEN 'downward_trend'
            ELSE 'mixed_trend'
        END as revenue_trend,
        -- Ranking within category
        RANK() OVER (PARTITION BY product_category ORDER BY last_month_revenue DESC) as revenue_rank_in_category,
        PERCENT_RANK() OVER (PARTITION BY product_category ORDER BY last_month_revenue) as revenue_percentile_in_category
    FROM pivoted_sales
)
SELECT 
    product_category,
    region,
    last_month_revenue,
    ROUND(mom_growth_percent, 2) as mom_growth_percent,
    ROUND(qoq_growth_percent, 2) as qoq_growth_percent,
    ROUND(yoy_growth_percent, 2) as yoy_growth_percent,
    revenue_trend,
    revenue_rank_in_category,
    ROUND(revenue_percentile_in_category * 100, 1) as revenue_percentile_in_category,
    -- Performance indicators
    CASE 
        WHEN mom_growth_percent > 10 THEN 'high_growth'
        WHEN mom_growth_percent > 0 THEN 'positive_growth'
        WHEN mom_growth_percent > -10 THEN 'slight_decline'
        ELSE 'significant_decline'
    END as performance_indicator
FROM analytics_metrics
WHERE last_month_revenue > 0
ORDER BY product_category, last_month_revenue DESC 