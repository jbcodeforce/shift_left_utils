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
    ddl_content: Optional[str] = None
    table_name: Optional[str] = None
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
        self.qwen_model_name=os.getenv("SL_LLM_MODEL","qwen3-coder:30b")
        self.mistral_model_name=os.getenv("SL_LLM_MODEL","mistral-small:latest")
        self.cogito_model_name=os.getenv("SL_LLM_MODEL","cogito:32b")
        self.kimi_k2_model_name=os.getenv("SL_LLM_MODEL","moonshotai/Kimi-K2-Instruct:novita")
        self.model_name=self.qwen_model_name
        self.llm_base_url=os.getenv("SL_LLM_BASE_URL","http://localhost:11434/v1")
        self.llm_api_key=os.getenv("SL_LLM_API_KEY","ollama_local_key")
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
                            test_case_name: str = None) -> dict[str, OutputTestData]:
        """
        Update synthetic test data across input SQL statements to ensure data consistency for joins and primary keys.

        This function uses an LLM to generate or update insert SQL statements for all input tables in a test case,
        ensuring that the generated data is consistent with the join logic and primary key constraints defined in the DML under test.

        Args:
            base_table_path (str): The base path to the table folder containing test files.
            dml_content (str): The DML SQL content under test.
            test_definition (SLTestDefinition): The test suite definition containing test cases and input/output files.
            ddl_map (dict[str, str]): A mapping from table names to their DDL file paths.
            test_case_name (str, optional): The name of the specific test case to process. If None, all test cases are processed.

        Returns:
            dict[str, OutputTestData]: A dictionary table_name of OutputTestData objects containing updated SQL content for each input table.
        """
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
            output_data_list = {} # important to keep a default output for each table
            for input_data in test_case.inputs:
                file_name = from_pipeline_to_absolute(base_table_path + "/" + input_data.file_name)
                with open(file_name, "r") as f:
                    sql_content = f.read()
                logger.info(f"Input SQL content for {input_data.table_name} is:\n {sql_content}")
                input_test_data = InputTestData(
                    insert_sql_content=sql_content,
                    table_name=input_data.table_name
                )
                default_output = OutputTestData(
                    table_name=input_data.table_name,
                    file_name=file_name,
                    output_sql_content=sql_content)
                output_data_list[input_data.table_name]=default_output
                input_data_list.append(input_test_data)

            # The prompt needs to have all the input sql to get a better view of the existing data.
            prompt = self.data_consistency.format(
                dml_under_test_content=dml_content,
                structured_input="\n".join([data.insert_sql_content for data in input_data_list])
            )
            logger.info(f"Prompt for data consistency is:\n {prompt}")
            try:
                response = self.llm_client.chat.completions.parse(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format=OutputTestDataList
                )
                obj_response = response.choices[0].message
                if obj_response.parsed:
                    post_fix_unit_test = get_config().get("app").get("post_fix_unit_test", "_ut")
                    for output in obj_response.parsed.outputs:
                        _table_name = output.table_name
                        if _table_name.endswith(post_fix_unit_test):
                            _table_name = _table_name[: -len(post_fix_unit_test)]
                        _table_name = _table_name.split("/")[-1]
                        data = output_data_list.get(_table_name, None)
                        if data is not None:
                            data.output_sql_content = output.output_sql_content
                        else:
                            logger.warning(f"No output data found for table {output.table_name}")
                    return output_data_list
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
        logger.info(f"Prompt for data column type compliant is:\n {prompt}")
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
                input_list: dict[str, OutputTestData],
                dml_content: str,
                test_definition: SLTestDefinition,
                test_case_name: str = None) -> dict[str, OutputTestData]:
        """
        Update the synthetic data validation SQL for test cases.

        This function updates the validation SQL statements for the provided test cases to ensure
        that the validation logic is consistent with the generated synthetic data and DML content.
        If no specific test_case_name is provided, it processes all test cases in the test_definition.
        Otherwise, it updates the validation SQL for the specified test case.

        Args:
            base_table_path (str): The base path to the table directory.
            input_list (List[str]): List of input SQL content or OutputTestData objects for the test cases.
            dml_content (str): The DML SQL content used for generating test data.
            test_definition (SLTestDefinition): The test suite definition containing test cases.
            test_case_name (str, optional): The name of the specific test case to update. If None, all test cases are processed.

        Returns:
            dict[str,OutputTestData]: A list of OutputTestData objects with updated validation SQL content.
        """
        validation_content = ""
        if test_case_name is None:
            accumulatedOutputTestData = {}
            for test_case in test_definition.test_suite:
                output_data = self._update_synthetic_data_validation_sql(base_table_path, input_list, dml_content, test_definition, test_case.name)
                accumulatedOutputTestData.update(output_data)
            return accumulatedOutputTestData
        else:
            test_case = next((tc for tc in test_definition.test_suite if tc.name == test_case_name), None)
            validation_fname = from_pipeline_to_absolute(base_table_path + "/" + test_case.outputs[0].file_name)
            with open(validation_fname, "r") as f:
                validation_content = f.read()
            input_sqls = '\n'.join([input_data.output_sql_content for input_data in input_list.values()])
            prompt = self.data_validation_sql_update.format(
                dml_content=dml_content,
                validation_content=validation_content,
                input_tables=input_sqls
            )
            logger.info(f"Prompt for data validation sql is:\n {prompt}")
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
                return {validation_output.table_name: validation_output}
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
        for  output_table_name, output_data in output_data_list.items():
            ddl_file_name = ddl_map.get(output_table_name, "")
            output_sql_content = output_data.output_sql_content
            logger.info(f"Update insert sql content for {output_sql_content}")
            with open(ddl_file_name, "r") as f:
                ddl_content = f.read()
                update_sql_content = self._update_synthetic_data_column_type_compliant(ddl_content, output_sql_content)
                output_data_list[output_table_name].output_sql_content = update_sql_content
                #output_data.file_name = ddl_file_name

        validation_output = self._update_synthetic_data_validation_sql(base_table_path, output_data_list, dml_content, test_definition, test_case_name)
        output_data_list.update(validation_output)
        return output_data_list.values()
