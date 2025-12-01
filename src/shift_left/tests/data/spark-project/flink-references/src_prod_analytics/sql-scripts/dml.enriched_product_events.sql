INSERT INTO enriched_product_events
WITH raw_events AS (
  SELECT product_id,
         category,
         event_timestamp,
         user_id,
         event_type,
         revenue,
         ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_timestamp DESC) as latest_event_rank,
         LAG(revenue, 1) OVER (PARTITION BY product_id ORDER BY event_timestamp) as prev_revenue,
         SUM(revenue) OVER (PARTITION BY product_id ORDER BY event_timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_revenue,
         RANK() OVER (PARTITION BY category ORDER BY revenue DESC) as revenue_rank_in_category,
         DATE_FORMAT(event_timestamp, 'yyyy-MM-dd') as event_date,
         HOUR(event_timestamp) as event_hour,
         DAYOFWEEK(event_timestamp) as day_of_week,
         CASE
           WHEN event_type = 'purchase' THEN revenue
           ELSE 0
         END as purchase_revenue,
         CASE
           WHEN HOUR(event_timestamp) BETWEEN 9 AND 17 THEN 'business_hours'
           ELSE 'off_hours'
         END as time_category
  FROM raw_product_events
  WHERE event_timestamp >= $ROWTIME - INTERVAL '30' DAY
    AND revenue > 0
)
SELECT product_id,
       category,
       event_timestamp,
       user_id,
       event_type,
       revenue,
       latest_event_rank,
       prev_revenue,
       rolling_7day_revenue,
       revenue_rank_in_category,
       event_date,
       event_hour,
       day_of_week,
       purchase_revenue,
       time_category
FROM raw_events
ORDER BY product_id, event_timestamp DESC;
