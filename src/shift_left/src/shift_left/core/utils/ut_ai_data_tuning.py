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
from typing import Optional, List



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
    table_name: Optional[str] = None
    file_name: Optional[str] = None
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
        print(f"Test content tuning using {self.model_name} model from {self.llm_base_url} and {self.llm_api_key[:25]}...")
        self.llm_client = OpenAI(api_key=self.llm_api_key, base_url=self.llm_base_url)
        self._load_prompts()

    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.core.utils.prompts.unit_tests").joinpath("data_consistency_cross_inserts.txt")
        with fname.open("r") as f:
            self.data_consistency= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.unit_tests").joinpath("data_column_type_compliant.txt")
        with fname.open("r") as f:
            self.data_column_type_compliant= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.unit_tests").joinpath("data_validation_sql_update.txt")
        with fname.open("r") as f:
            self.data_validation_sql_update= f.read()

    def _update_synthetic_data_cross_statements(self, 
                            base_table_path: str, 
                            dml_content: str, 
                            test_definition: SLTestDefinition, 
                            ddl_map: dict[str, str],
                            test_case_name: str = None) -> list[OutputTestData]:
        if test_case_name is None:
            accumulatedOutputStatements = []
            for test_case in test_definition.test_suite:
                output_data = self._update_synthetic_data_cross_statements(base_table_path, dml_content, test_definition, test_case.name)
                accumulatedOutputStatements.extend(output_data)
            return accumulatedOutputStatements
        else:
            test_case = next((tc for tc in test_definition.test_suite if tc.name == test_case_name), None)
         
            # Create structured input data
            input_data_list = []
            output_data_list = []
            for input_data in test_case.inputs:
                file_name = from_pipeline_to_absolute(base_table_path + "/" + input_data.file_name)
                with open(file_name, "r") as f:
                    sql_content = f.read()
                logger.info(f"Input SQL content for {input_data.table_name} is {sql_content}")
                # Get DDL content for this input table
                ddl_file_name = ddl_map.get(input_data.table_name, "")
                with open(ddl_file_name, "r") as f:
                    ddl_content = f.read()
                input_test_data = InputTestData(
                    ddl_content=ddl_content,
                    insert_sql_content=sql_content,
                    table_name=input_data.table_name
                )
                default_output = OutputTestData(
                    table_name=input_data.table_name, 
                    file_name=file_name, 
                    output_sql_content=sql_content)
                output_data_list.append(default_output)
                input_data_list.append(input_test_data)
            
            # Create structured prompt data
            structured_input = {
                "dml_under_test": dml_content,
                "input_tables": [data.model_dump() for data in input_data_list]
            }
            
            prompt = self.data_consistency.format(
                dml_under_test_content=dml_content, 
                structured_input=structured_input
            )
            try:
                response = self.llm_client.chat.completions.parse(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format=OutputTestDataList
                )
                obj_response = response.choices[0].message  
                if obj_response.parsed:
                    return obj_response.parsed.outputs
                else:
                    return output_data_list
            except Exception as e:
                print(f"Error: {e}")
                return output_data_list 

    def _update_synthetic_data_column_type_compliant(self, ddl_content: str, sql_content: str) -> str:
        prompt = self.data_column_type_compliant.format(
            ddl_content=ddl_content, 
            sql_content=sql_content
        )
        
        try:
            response = self.llm_client.chat.completions.parse(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format=OutputTestData
            )
            obj_response = response.choices[0].message  
            if obj_response.parsed:
                return obj_response.parsed.output_sql_content
            else:
                return sql_content
        except Exception as e:
            print(f"Error: {e}")
            return sql_content



    def _update_synthetic_data_validation_sql(self,
                base_table_path: str,  
                input_list: List[str], 
                dml_content: str, 
                test_definition: SLTestDefinition, 
                test_case_name: str = None) -> list[OutputTestData]:
        """
        """
        validation_content = ""
        if test_case_name is None:
            accumulatedOutputStatements = []
            for test_case in test_definition.test_suite:
                output_data = self._update_synthetic_data_validation_sql(base_table_path, input_list, dml_content, test_definition, test_case.name)
                accumulatedOutputStatements.extend(output_data)
            return accumulatedOutputStatements
        else:
            test_case = next((tc for tc in test_definition.test_suite if tc.name == test_case_name), None)
            validation_fname = from_pipeline_to_absolute(base_table_path + "/" + test_case.outputs[0].file_name)
            with open(validation_fname, "r") as f:
                validation_content = f.read()
            input_sqls = [input_data.output_sql_content for input_data in input_list]
            prompt = self.data_validation_sql_update.format(
                dml_content=dml_content, 
                validation_content=validation_content,
                input_tables=input_sqls
            )
            validation_output = OutputTestData(table_name=test_case.outputs[0].table_name, 
                file_name=validation_fname, 
                output_sql_content=validation_content)
        
            try:
                response = self.llm_client.chat.completions.parse(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format=OutputTestData
                )
                obj_response = response.choices[0].message  
                if obj_response.parsed:
                    validation_output.output_sql_content=obj_response.parsed.output_sql_content
                    return [validation_output]
                else:
                    return [validation_content]
            except Exception as e:
                print(f"Error: {e}")
                return [validation_content]
        

    def enhance_test_data(self, 
                base_table_path: str,
                dml_content: str, 
                test_definition: SLTestDefinition, 
                test_case_name: str = None) -> list[OutputTestData]:
        """
        Get over all in the insert sql content of all test cases to keep consitency
        between data and compliance with the ddl of the input tables.
        This involves orchestrating different agents. The input synthetic data was generated
        without AI as it does not needs bigger machine.
        This method uses the LLM to enhance the synthetic data. The steps are:
        1. Using the dml under test and the n input data represented by insert .sql files, consider
        the joins and modify the data between all inputs.
        2. For each test case get the ddl definition and the insert sql content, and validate
        if the data conforms with the ddl.
        If not, the LLM will be used to generate new data, and this is repeated until 
        """
        ddl_map = {foundation.table_name: from_pipeline_to_absolute(base_table_path + "/" + foundation.ddl_for_test) for foundation in test_definition.foundations} 
        output_data_list = self._update_synthetic_data_cross_statements(base_table_path, dml_content, test_definition, ddl_map, test_case_name)
        for output_data in output_data_list:
            ddl_file_name = ddl_map.get(output_data.table_name, "")
            logger.info(f"Update insert sql content for {output_data.output_sql_content}")
            update_sql_content = self._update_synthetic_data_column_type_compliant(ddl_file_name, output_data.output_sql_content)
            output_data.output_sql_content = update_sql_content
            output_data.file_name = ddl_file_name

        validation_output = self._update_synthetic_data_validation_sql(base_table_path, output_data_list, dml_content, test_definition, test_case_name)
        output_data_list.extend(validation_output)
        return output_data_list