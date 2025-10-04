INSERT INTO app_orders
select 
    window_start, 
    window_end, 
    customer_id, sum(order_amount) 
from table(tumble(table `daily_spend`, DESCRIPTOR(tx_timestamp), interval '24' hours)) 
group by window_start, window_end, customer_id 