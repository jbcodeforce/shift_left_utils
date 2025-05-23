
from ollama import chat, AsyncClient
from pydantic import BaseModel
import os
import asyncio

class FlinkSql(BaseModel):
    sql: str

translator_system_prompt= """
you are an agent expert in Apache Flink SQL and Confluent ksql.

Translate the following ksqlDB DDL script into Apache Flink SQL for real-time processing.

* Do not add suggestions or explanations in the response, just return the structured Flink sql output.
* Do not use VARCHAR prefer STRING. 
* Use CREATE TABLE IF NOT EXISTS instead of CREATE TABLE
* use an INSERT INTO statement to continuously write to the table when the source is a stream
* When the sql contains LATEST_BY_OFFSET(column_name), it should be translated to column_name in a DML statement
* The KSQL LATEST_BY_OFFSET combined with GROUP BY dbTable on a stream effectively creates a materialized view (a table) where for each unique dbTable, the latest values for all other columns are maintained.
* consider stream in ksql to be translated as a table in flink

when ksql has the following statement: 
CREATE TABLE KPI_CONFIG_TABLE WITH (KAFKA_TOPIC='stage.kpi_config_table', VALUE_FORMAT='JSON_SR') AS SELECT dbTable
translate it to: CREATE TABLE KPI_CONFIG_TABLE 
           with dbTable as (select * from dbTable)

Use back quote character like ` around column name which is one of the SQL keyword. As an example a column name should be `name`. 

Finish the statement with the following declaration:
   PRIMARY KEY(sid) NOT ENFORCED -- VERIFY KEY
) DISTRIBUTED BY HASH(`sid`) INTO 6 BUCKETS WITH ( 
   'changelog.mode' = 'retract',
   'kafka.cleanup-policy'= 'compact',
   'key.avro-registry.schema-context' = '.flink-dev',
   'value.avro-registry.schema-context' = '.flink-dev',
   'key.format' = 'avro-debezium-registry',
   'value.format' = 'avro-debezium-registry',
   'kafka.retention.time' = '0',
   'kafka.producer.compression.type' = 'snappy',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'value.fields-include' = 'all'
)
"""

translator_prompt_template = """
ksql_input: {sql_input}
"""

class KsqlToFlinkSqlAgent:

    def __init__(self):
        #self.model_name=os.getenv("LLM_MODEL","qwen2.5-coder:32b")
        self.model_name=os.getenv("LLM_MODEL","mistral-small:latest")
        self.llm_base_url=os.getenv("LLM_BASE_URL","http://localhost:11434")

    async def _translate_from_ksql_to_flink_sql(self, sql: str) -> str:
        client = AsyncClient()
        response = await client.generate(self.model_name,
                                         FlinkSql, 
                                         messages=[
                                            {"role": "system", "content": translator_system_prompt},
                                            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
                                        ]) 
        return response['response']

    def translate_from_ksql_to_flink_sql(self, sql: str) -> str:
        parts = ""
        messages=[
            {"role": "system", "content": translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        response= chat(model=self.model_name, 
                        format=FlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = FlinkSql.model_validate_json(response.message.content)
        return obj_response.sql





