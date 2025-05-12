-- macros/dedup.sql
{% macro dedup(table_name, primary_key) %}
with dedup as (

    select
    {%- if primary_key == '' %}
        row_number() over(partition by __db order by __source_ts_ms desc, __ts_ms desc, case when __op = 'c' then 1 when __op = 'd' then 2 when __op = 'u' then 3 else 4 end asc) as row_num
    {%- else %}
        row_number() over(partition by __db, {{primary_key}} order by __source_ts_ms desc, __ts_ms desc, case when __op = 'c' then 1 when __op = 'd' then 2 when __op = 'u' then 3 else 4 end asc) as row_num
    {%- endif %}
        ,*
    from {{table_name}}
)

,final as (

    select
        *
    from dedup
    where row_num = 1 and __op <> 'd'

)

select * from final
{% endmacro %}