# Unit tests explanations

The `c360_dim_groups` uses 2 input tables as sources and generates record with the No primary key found in the statement. primary keys

## DML analysis


The joins are unbounded leading the Flink state growth.

These JOINs will accumulate unlimited state:
```sql

```


## Real data analysis

Running source data analysis, from the env-nknqp3 environment:

| Table Name | # messages in topic | Information of interest |
|------------|------------|--------------|
| src_c360_groups |  |  |
| src_common_tenant |  |  |


## Unit tests creation and execution:

DDL -> 

| UT |   Inserts | Validation |
| --- | --- | --- |
| sql | ✅ | ✅  |

### Issues to address



### src_c360_groups

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from src_c360_groups  group by id, tenant_id
```


### src_common_tenant

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from src_common_tenant  group by id, tenant_id
```

