## Fact Table: aggregation

Status date: 11/2025

The ksql are:
```
CREATE STREAM "daily-spend" WITH (KAFKA_TOPIC='app.dev.daily-spend', VALUE_FORMAT='JSON_SR');    
/* Create stream for destination topic */
CREATE STREAM "orders" WITH (KAFKA_TOPIC='app.dev.orders', VALUE_FORMAT='JSON_SR') 
AS SELECT customer_id, sum(order_amount) FROM "daily-spend" WINDOW TUMBLING (SIZE 24 HOUR) 
WHERE order_amount > 0 GROUP BY customer_id 
EMIT CHANGES;
```

Which translate to the 
* orders ddl
* daily-spend ddl
* dml to compute aggregate over tumbling windows

```sql
INSERT INTO orders
select
    window_start,
    window_end,
    customer_id,
    sum(order_amount) as order_amount_sum
from table(tumble(table `daily_spend`, DESCRIPTOR(tx_timestamp), interval '24' hours))
group by window_start, window_end, customer_id
```

## Tests

The DDL and DML were tested within Confluent cloud Flink.

* Create daily_spend
* Insert test records using the sql in tests
* Create oders
* Execute the dml.aggregation.sql
* Verify result

Or execute the select part of the dml

```
select
    window_start,
    window_end,
    customer_id,
    sum(order_amount) as order_amount_sum
from table(tumble(table `daily_spend`, DESCRIPTOR(tx_timestamp), interval '24' hours))
group by window_start, window_end, customer_id
```

* clean up
```
drop table daily_spend;
```
