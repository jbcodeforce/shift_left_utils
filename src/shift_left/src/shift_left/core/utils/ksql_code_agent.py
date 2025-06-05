
from ollama import chat, AsyncClient
from pydantic import BaseModel
from typing import Tuple
import os

from shift_left.core.utils.app_config import get_config
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement

# use structured output to get the sql from LLM output
class FlinkSql(BaseModel):
    sql: str

translator_system_prompt= """
you are an agent expert in Apache Flink SQL and Confluent kSQL.
Your goal is to translate a ksqlDB script into Apache Flink SQL for real-time processing.

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

flink_sql_syntax_checker_system_prompt = """
you are an agent expert in Apache Flink SQL syntax.
Your goal is to check the syntax of the following Apache Flink SQL statement.
amd generate a syntaxically valid SQL script.

When the sql starts with CREATE TABLE, be sure to replace existing WITH clause with the following declaration:
   PRIMARY KEY(sid) NOT ENFORCED -- VERIFY KEY
) DISTRIBUTED BY HASH(`sid`) INTO 6 BUCKETS WITH ( 
   'changelog.mode' = 'append',
   'kafka.cleanup-policy'= 'compact',
   'key.avro-registry.schema-context' = '.flink-dev',
   'value.avro-registry.schema-context' = '.flink-dev',
   'key.format' = 'avro-registry',
   'value.format' = 'avro-registry',
   'kafka.retention.time' = '0',
   'kafka.producer.compression.type' = 'snappy',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
   'value.fields-include' = 'all'
)
"""
syntax_checker_prompt_template = """
flink_sql_input: {sql_input}
history of the conversation: {history}
"""

class KsqlToFlinkSqlAgent:

    def __init__(self):
        #self.model_name=os.getenv("LLM_MODEL","qwen2.5-coder:32b")
        self.model_name=os.getenv("LLM_MODEL","mistral-small:latest")
        self.llm_base_url=os.getenv("LLM_BASE_URL","http://localhost:11434")


    def _validate_flink_sql_on_cc(self, sql: str) -> Tuple[bool, str]:
        config = get_config()
        compute_pool_id = config['flink']['compute_pool_id']
        statement_name = "validate-flink-sql"
        statement = post_flink_statement(compute_pool_id, statement_name, sql)
        delete_statement_if_exists(statement_name)
        print(f"Statement: {statement}")
        if statement and isinstance(statement, Statement) and statement.status:
            if statement.status.phase == "RUNNING":
                return True, ""
            else:
                return False, statement.status.detail
        else:
            return False, statement
        
        

    def _translator_agent(self, sql: str) -> str:
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

    def _syntax_checker_agent(self, sql: str, history: str) -> str:
        messages=[
            {"role": "system", "content": flink_sql_syntax_checker_system_prompt},
            {"role": "user", "content": syntax_checker_prompt_template.format(sql_input=sql, history=history)}
        ]
        response= chat(model=self.model_name, 
                        format=FlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = FlinkSql.model_validate_json(response.message.content)
        return obj_response.sql

    def translate_from_ksql_to_flink_sql(self, ksql: str) -> str:
        """
        Entry point to translate ksql to flink sql
        """
        agent_history = []
        final_sql_created = False
        iteration_count = 0
        translated_sql = self._translator_agent(ksql)
        print(f"Done with translator agent {translated_sql}")
        while not final_sql_created and iteration_count < 3:
            iteration_count += 1
            final_sql = self._syntax_checker_agent(translated_sql, agent_history)
            print(f"Process with syntax checker agent {iteration_count}")
            agent_history.append({"agent": "syntax_checker", "sql": final_sql})
                    
        return final_sql





