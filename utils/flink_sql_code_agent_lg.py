import operator
import os, argparse
from datetime import datetime
from typing import Annotated, TypedDict, Union, Tuple

from pathlib import Path
from langchain_core.agents import AgentAction, AgentFinish
from langchain_core.messages import BaseMessage
from langchain_core.tools import tool
from langchain.agents import create_react_agent
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolExecutor, ToolInvocation
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama.llms import OllamaLLM
from langchain_core.output_parsers import StrOutputParser
import re

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Test Flink sql generation from a dbt sql file'
)
parser.add_argument('-n', '--file_path', required=True, help="name of the fpath withthe sql file to process")

"""
Create Flink SQL with a set of agents working within the following workflow

1. translate (dbt sql string to the matching Fink sql )
2. remove some sql line we do not need in the flink sql as the dml is insert into table ...
3. be sure the Flink SQL is syntactically correct
4. Generate the matching DDL for the dml
"""

def extract_sql_blocks(text) -> str:
    """
    Extract SQL code blocks from a given text.
    
    Args:
        text (str): The input text containing SQL code blocks
    
    Returns:
        list: A list of SQL code blocks found in the text
    """
    if text.find("```sql") > 0:
        sql_blocks = re.findall(r'```sql\n(.*?)```', text, re.DOTALL) 
        sql_block = ""
        for line in sql_blocks: # back in one string
            sql_block += line
        return sql_block
    else:
        return text

model = OllamaLLM(model="qwen2.5-coder:32b")

translator_template = """
you are qwen-coder an agent expert in Apache Flink SQL and  DBT (Data Build Tool). 
You need to translate the dbt sql statement given as sql input and transform it into a
Flink SQL statement with same semantic.

Keep all the select statement defined with WITH keyword.

Do not add suggestions or explanations in the response, just return the structured sql output.

Start the generated code with:

INSERT INTO {table_name}

Question: {sql_input}

"""

flink_sql_syntaxic_template="""
you are qwen-coder an agent expert in Apache Flink SQL syntax expert and your goal is to generate a cleaner Apache Flink SQL
statement from the following {flink_sql}.

Do not generate explanations for the fixes you did.
"""

flink_ddl_generation_template="""
you are qwen-coder an agent expert in Apache Flink SQL. Your goal is to build a CREATE TABLE Apache Flink SQL
statement from the current flink sql insert into statement like:

{flink_sql}

The resulting ddl for the table {table_name} should include STRING for any _id column
and VARCHAR as STRING. 

Finish the statement with the following declaration:

   PRIMARY KEY(ID) NOT ENFORCED -- VERIFY KEY
) WITH ( 'changelog.mode' = 'upsert',
  'value.format' = 'avro-registry',
   'value.fields-include' = 'all'
)

Do not generate explanations for the fixes you did.
"""

def remove_noise_in_sql(sql: str) -> str:
    """
    remove final AS ( and SELECT * FROM final
    """
    newone=sql.replace("final AS (","").replace("CURRENT_DATE()", "event_ts")
    return newone.replace("SELECT * FROM final","")


class AgentState(TypedDict):
    sql_input: str
    table_name: str
    flink_sql: str
    derived_ddl: str


    
def define_flink_sql_agent():
    
    def run_translator_agent(state):
        """
        change the sql_input string to the matching flink sql statement
        """
        print(f"\n--- Start translator Agent ---")
        prompt = ChatPromptTemplate.from_template(translator_template) 
        chain = prompt | model 
        llm_out = chain.invoke(state)
        flink_sql = remove_noise_in_sql(llm_out)
        print(f"\n--- Done translator Agent: \n{flink_sql}")
        return {"flink_sql": flink_sql}

    def syntax_cleaner(state):
        """
        Verify the Flink SQL syntax is correct
        """
        print(f"\n--- Start syntax_cleaner Agent ---")
        prompt = ChatPromptTemplate.from_template(flink_sql_syntaxic_template) 
        chain = prompt | model 
        agent_outcome = chain.invoke(state)
        print(f" --- Done Syntax cleaner Agent:\n{agent_outcome}")
        return {"flink_sql": agent_outcome}

    def ddl_generation(state):
        print(f"\n--- Start ddl_generation Agent ---")
        prompt = ChatPromptTemplate.from_template(flink_ddl_generation_template) 
        chain = prompt | model 
        derived_ddl = chain.invoke(state)
        print(f" --- Done DDL generator Agent:\n{derived_ddl}")
        return {"derived_ddl": derived_ddl}

    def clean_sqls(state):
        print(f"\n--- Start clean_sql Agent ---")
        sql = state["flink_sql"]
        better_sql=extract_sql_blocks(sql)
        print(f" --- Done Clean SQL Agent: \n{better_sql}")
        return {"flink_sql": better_sql}
    

    workflow = StateGraph(AgentState)
    workflow.add_node("translator", run_translator_agent)
    workflow.add_node("cleaner", clean_sqls)
    #workflow.add_node("syntaxic_cleaner", syntax_cleaner)
    workflow.add_node("ddl_generation", ddl_generation)
    workflow.set_entry_point("translator")

    workflow.add_edge("translator", "cleaner")
    #workflow.add_edge("syntaxic_cleaner", "ddl_generation")
    workflow.add_edge("cleaner","ddl_generation")
    workflow.add_edge("ddl_generation", END)
    app = workflow.compile()
    return app


def translate_to_flink_sqls(table_name: str, sql_content: str) -> Tuple[str,str]:
    app = define_flink_sql_agent()
    inputs = {"sql_input": sql_content, "table_name" : table_name}
    result=app.invoke(inputs)
    return result['flink_sql'], result['derived_ddl']
 


if __name__ == "__main__":
    args = parser.parse_args()
    source_path=args.file_path
    the_path= Path(source_path)
    table_name = the_path.stem
    with open(source_path) as f:
        sql_content= f.read()
        print(f"--- Input file {source_path} read ")
        a,b=translate_to_flink_sqls(table_name,sql_content)
        print(a)
        print(" ----- ")
        print(b)
    print("-"*40)
    print("done !")
