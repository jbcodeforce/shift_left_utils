-- Advanced Transformations with UDFs and Complex Data Manipulations
WITH enriched_data AS (
    SELECT 
        transaction_id,
        user_id,
        product_id,
        transaction_timestamp,
        amount,
        currency,
        merchant_category,
        payment_method,
        -- JSON parsing and manipulation
        GET_JSON_OBJECT(transaction_metadata, '$.device.fingerprint') as device_fingerprint,
        GET_JSON_OBJECT(transaction_metadata, '$.location.ip_address') as ip_address,
        GET_JSON_OBJECT(transaction_metadata, '$.risk_scores.fraud_score') as fraud_score,
        -- Custom string transformations
        REGEXP_REPLACE(merchant_category, '[^a-zA-Z0-9]', '_') as clean_merchant_category,
        SPLIT(GET_JSON_OBJECT(transaction_metadata, '$.tags'), ',') as transaction_tags,
        -- Advanced date manipulations
        UNIX_TIMESTAMP(transaction_timestamp) as unix_timestamp,
        FROM_UNIXTIME(UNIX_TIMESTAMP(transaction_timestamp), 'yyyy-MM-dd HH:mm:ss') as formatted_timestamp,
        WEEKOFYEAR(transaction_timestamp) as week_number,
        -- Currency conversion simulation (would be UDF in real scenario)
        CASE 
            WHEN currency = 'EUR' THEN amount * 1.1
            WHEN currency = 'GBP' THEN amount * 1.25
            WHEN currency = 'JPY' THEN amount * 0.0075
            ELSE amount
        END as amount_usd,
        -- Risk scoring (complex business logic)
        CASE 
            WHEN CAST(GET_JSON_OBJECT(transaction_metadata, '$.risk_scores.fraud_score') AS DOUBLE) > 0.8 THEN 'high_risk'
            WHEN CAST(GET_JSON_OBJECT(transaction_metadata, '$.risk_scores.fraud_score') AS DOUBLE) > 0.5 THEN 'medium_risk'
            ELSE 'low_risk'
        END as risk_category,
        -- Advanced mathematical operations
        LOG10(amount + 1) as log_amount,
        SQRT(amount) as sqrt_amount,
        POW(amount, 0.5) as power_transform,
        -- Percentile-based transformations
        NTILE(10) OVER (PARTITION BY merchant_category ORDER BY amount) as amount_decile,
        PERCENT_RANK() OVER (PARTITION BY user_id ORDER BY transaction_timestamp) as user_transaction_percentile
    FROM raw_transactions
    WHERE transaction_timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
),
feature_engineering AS (
    SELECT 
        user_id,
        transaction_id,
        amount_usd,
        risk_category,
        clean_merchant_category,
        transaction_timestamp,
        -- User behavior features
        COUNT(*) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as transactions_last_7,
        SUM(amount_usd) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as spending_last_7,
        AVG(amount_usd) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as avg_spending_30d,
        STDDEV(amount_usd) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as stddev_spending_30d,
        -- Time-based features
        DATEDIFF(transaction_timestamp, 
                LAG(transaction_timestamp) OVER (PARTITION BY user_id ORDER BY transaction_timestamp)) as days_since_last_transaction,
        COUNT(DISTINCT clean_merchant_category) OVER (
            PARTITION BY user_id ORDER BY transaction_timestamp 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as unique_merchants_30d,
        -- Velocity features
        CASE 
            WHEN LAG(transaction_timestamp) OVER (PARTITION BY user_id ORDER BY transaction_timestamp) IS NOT NULL
            THEN (UNIX_TIMESTAMP(transaction_timestamp) - 
                  UNIX_TIMESTAMP(LAG(transaction_timestamp) OVER (PARTITION BY user_id ORDER BY transaction_timestamp))) / 3600.0
            ELSE NULL
        END as hours_since_last_transaction,
        -- Z-score calculation for anomaly detection
        CASE 
            WHEN STDDEV(amount_usd) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                                         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0
            THEN (amount_usd - AVG(amount_usd) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                                                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) / 
                 STDDEV(amount_usd) OVER (PARTITION BY user_id ORDER BY transaction_timestamp 
                                         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
            ELSE 0
        END as amount_zscore,
        -- Categorical encoding
        DENSE_RANK() OVER (ORDER BY clean_merchant_category) as merchant_category_encoded,
        -- Binary features
        CASE WHEN HOUR(transaction_timestamp) BETWEEN 22 AND 6 THEN 1 ELSE 0 END as is_late_night,
        CASE WHEN DAYOFWEEK(transaction_timestamp) IN (1, 7) THEN 1 ELSE 0 END as is_weekend,
        CASE WHEN amount_usd > AVG(amount_usd) OVER (PARTITION BY clean_merchant_category) * 3 THEN 1 ELSE 0 END as is_large_amount
    FROM enriched_data
),
ml_features AS (
    SELECT 
        user_id,
        transaction_id,
        -- Create interaction features
        amount_usd * transactions_last_7 as amount_velocity_interaction,
        CASE 
            WHEN days_since_last_transaction IS NOT NULL AND days_since_last_transaction > 0
            THEN amount_usd / days_since_last_transaction
            ELSE 0
        END as amount_per_day_gap,
        -- Bucketing continuous variables
        CASE 
            WHEN amount_usd < 10 THEN 'micro'
            WHEN amount_usd < 100 THEN 'small'
            WHEN amount_usd < 1000 THEN 'medium'
            WHEN amount_usd < 10000 THEN 'large'
            ELSE 'very_large'
        END as amount_bucket,
        -- Risk score combinations
        CASE 
            WHEN risk_category = 'high_risk' AND is_late_night = 1 THEN 'high_risk_night'
            WHEN risk_category = 'high_risk' AND is_weekend = 1 THEN 'high_risk_weekend'
            WHEN risk_category = 'medium_risk' AND is_large_amount = 1 THEN 'medium_risk_large'
            ELSE CONCAT(risk_category, '_normal')
        END as composite_risk_category,
        -- Ratios and derived metrics
        CASE 
            WHEN avg_spending_30d > 0 
            THEN amount_usd / avg_spending_30d 
            ELSE 0 
        END as amount_to_avg_ratio,
        CASE 
            WHEN stddev_spending_30d > 0 
            THEN ABS(amount_zscore) 
            ELSE 0 
        END as amount_deviation_score,
        -- Time-based aggregations
        SUM(CASE WHEN is_late_night = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY user_id ORDER BY transaction_timestamp 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as late_night_transactions_30d,
        -- Advanced pattern detection
        CASE 
            WHEN hours_since_last_transaction IS NOT NULL 
                 AND hours_since_last_transaction < 1 
                 AND amount_usd > avg_spending_30d * 2
            THEN 1 
            ELSE 0 
        END as rapid_high_spending_flag
    FROM feature_engineering
),
aggregated_insights AS (
    SELECT 
        user_id,
        -- User-level aggregations
        COUNT(*) as total_transactions,
        SUM(amount_usd) as total_spending,
        AVG(amount_usd) as avg_transaction_amount,
        STDDEV(amount_usd) as spending_volatility,
        -- Risk aggregations
        SUM(CASE WHEN risk_category = 'high_risk' THEN 1 ELSE 0 END) as high_risk_transactions,
        AVG(amount_deviation_score) as avg_anomaly_score,
        MAX(amount_deviation_score) as max_anomaly_score,
        -- Behavioral patterns
        AVG(rapid_high_spending_flag) as rapid_spending_rate,
        COUNT(DISTINCT amount_bucket) as spending_diversity,
        COUNT(DISTINCT composite_risk_category) as risk_pattern_diversity,
        -- Time patterns
        AVG(late_night_transactions_30d) as avg_late_night_activity,
        STDDEV(CASE WHEN days_since_last_transaction IS NOT NULL 
                   THEN days_since_last_transaction ELSE NULL END) as transaction_timing_variance
    FROM ml_features
    GROUP BY user_id
)
SELECT 
    user_id,
    total_transactions,
    ROUND(total_spending, 2) as total_spending,
    ROUND(avg_transaction_amount, 2) as avg_transaction_amount,
    ROUND(spending_volatility, 2) as spending_volatility,
    high_risk_transactions,
    ROUND(avg_anomaly_score, 3) as avg_anomaly_score,
    ROUND(rapid_spending_rate, 3) as rapid_spending_rate,
    spending_diversity,
    risk_pattern_diversity,
    -- Final risk scoring
    CASE 
        WHEN high_risk_transactions > total_transactions * 0.3 
             AND avg_anomaly_score > 1.5 
        THEN 'user_high_risk'
        WHEN rapid_spending_rate > 0.1 
             AND spending_volatility > avg_transaction_amount 
        THEN 'user_medium_risk'
        ELSE 'user_low_risk'
    END as final_user_risk_category,
    -- Behavioral classification
    CASE 
        WHEN spending_diversity <= 2 AND transaction_timing_variance < 1 THEN 'routine_spender'
        WHEN spending_diversity > 4 AND avg_late_night_activity > 2 THEN 'diverse_night_spender'
        WHEN avg_transaction_amount > 1000 AND total_transactions < 10 THEN 'high_value_occasional'
        ELSE 'regular_spender'
    END as spending_behavior_type
FROM aggregated_insights
WHERE total_transactions >= 5  -- Only users with sufficient transaction history
ORDER BY total_spending DESC, avg_anomaly_score DESC 