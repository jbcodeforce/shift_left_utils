"""
Copyright 2024-2025 Confluent, Inc.
"""

from shift_left.core.utils.app_config import get_config, logger
import os
from openai import OpenAI
import importlib.resources
from shift_left.core.models.flink_test_model import SLTestDefinition, SLTestCase, TestResult, TestSuiteResult
from shift_left.core.utils.sql_parser import SQLparser
from pydantic import BaseModel
from shift_left.core.utils.file_search import from_pipeline_to_absolute
from shift_left.core.utils.app_config import get_config, logger

class InputTestData(BaseModel):
    """
    Input test data for one unit test.
    """
    ddl_content: str
    table_name: str
    insert_sql_content: str

class OutputTestData(BaseModel):
    """
    Input test data for one unit test.
    """
    table_name: str
    output_sql_content: str

class OutputTestDataList(BaseModel):
    """
    List of output test data for one unit test.
    """
    outputs: list[OutputTestData]

class AIBasedDataTuning:
    """
    Given unit test definition, this class will use LLM to generate the data for the unit tests.
    The main dml to test is given as input. The data generated need to be consistent with the join
    conditions of the main dml and the primary keys of the tables.
    """

    def __init__(self):
        self.qwen_model_name=os.getenv("SL_LLM_MODEL","qwen2.5-coder:32b")
        self.mistral_model_name=os.getenv("SL_LLM_MODEL","mistral-small:latest")
        self.cogito_model_name=os.getenv("SL_LLM_MODEL","cogito:32b")
        self.kimi_k2_model_name=os.getenv("SL_LLM_MODEL","moonshotai/Kimi-K2-Instruct:novita")
        self.model_name=self.qwen_model_name
        self.llm_base_url=os.getenv("SL_LLM_BASE_URL","http://localhost:11434/v1")
        self.llm_api_key=os.getenv("SL_LLM_API_KEY","ollama_test_key")
        print(f"Using {self.model_name} with {self.llm_base_url} and {self.llm_api_key[:25]}...")
        self.llm_client = OpenAI(api_key=self.llm_api_key, base_url=self.llm_base_url)

    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.core.utils.prompts.unit_tests").joinpath("main_instructions.txt")
        with fname.open("r") as f:
            self.main_instructions= f.read()

    def update_synthetic_data(self, dml_content: str, test_definition: SLTestDefinition, test_case_name: str):
        self._load_prompts()
        
        # Find the test case
        test_case = next((tc for tc in test_definition.test_suite if tc.name == test_case_name), None)
        
        # Build DDL mapping from foundations
        ddl_map = {foundation.table_name: foundation.ddl_for_test for foundation in test_definition.foundations}
        
        # Create structured input data
        input_data_list = []
        for input_data in test_case.inputs:
            file_name = from_pipeline_to_absolute(input_data.file_name)
            with open(file_name, "r") as f:
                sql_content = f.read()
            
            # Get DDL content for this table
            ddl_file_name = ddl_map.get(input_data.table_name, "")
            with open(ddl_file_name, "r") as f:
                ddl_content = f.read()
            input_test_data = InputTestData(
                ddl_content=ddl_content,
                insert_sql_content=sql_content,
                table_name=input_data.table_name
            )
            input_data_list.append(input_test_data)
        
        # Create structured prompt data
        structured_input = {
            "dml_under_test": dml_content,
            "input_tables": [data.model_dump() for data in input_data_list]
        }
        
        prompt = self.main_instructions.format(
            dml_under_test_content=dml_content, 
            structured_input=structured_input
        )
        print(prompt)
        try:
            response = self.llm_client.chat.completions.parse(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format=OutputTestDataList
            )
            print(response.choices[0])
            obj_response = response.choices[0].message  
            if obj_response.parsed:
                return obj_response.parsed.outputs
            else:
                return None
        except Exception as e:
            print(f"Error: {e}")
            return None 

    
    
            