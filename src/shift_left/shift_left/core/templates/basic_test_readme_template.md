# Unit tests explanations

The `{{ table_name }}` uses {{ num_input_tables }} input tables as sources and generates record with the {{ primary_keys }} primary keys

## DML analysis

{% if has_unbounded_joins %}
The joins are unbounded leading the Flink state growth.

These JOINs will accumulate unlimited state:
```sql
{{ unbounded_joins_sql }}
```
{% endif %}

## Real data analysis

Running source data analysis{% if environment %}, from the {{ environment }} environment{% endif %}:

| Table Name | # messages in topic | Information of interest |
|------------|------------|--------------|
{% for table in source_tables -%}
| {{ table }} |  |  |
{% endfor %}

## Unit tests creation and execution:

DDL -> 

| UT |   Inserts | Validation |
| --- | --- | --- |
| sql | ✅ | ✅  |

### Issues to address


{% for table_name in source_tables %}
### {{ table_name }}

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from {{ table_name }}  group by id, tenant_id
```

{% endfor %}