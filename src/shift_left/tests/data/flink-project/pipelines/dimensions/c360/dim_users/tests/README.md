# Unit tests explanations

The `sl_c360_dim_users` uses 2 input tables as sources and generates record with the No primary key found in the statement. primary keys

## DML analysis


The joins are unbounded leading the Flink state growth.

These JOINs will accumulate unlimited state:
```sql

```


## Real data analysis

Running source data analysis, from the env-nknqp3 environment:

| Table Name | # messages in topic | Information of interest |
|------------|------------|--------------|
| sl_c360_dim_groups |  |  |
| sl_c360_src_users |  |  |


## Unit tests creation and execution:

DDL -> 

| UT |   Inserts | Validation |
| --- | --- | --- |
| sql | ✅ | ✅  |

### Issues to address



### c360_dim_groups

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from sl_c360_dim_groups  group by id, tenant_id
```


### c360_src_users

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from sl_c360_src_users  group by id, tenant_id
```

