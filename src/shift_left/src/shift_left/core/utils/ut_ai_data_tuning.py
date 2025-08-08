"""
Copyright 2024-2025 Confluent, Inc.
"""

from shift_left.core.utils.app_config import get_config, logger
import os
from openai import OpenAI
import importlib.resources
from shift_left.core.test_mgr import SLTestDefinition, Foundation, SLTestData
from shift_left.core.utils.sql_parser import SQLparser
from pydantic import BaseModel



class SyntheticData(BaseModel):
    """
    Synthetic data for the unit tests.
    """
    dml_to_test_content: str
    current_synthetic_data: str
    synthetic_data_to_test: str

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
        fname = importlib.resources.files("shift_left.core.utils.prompts.ut_ai_data_tuning").joinpath("main_instructions.txt")
        with fname.open("r") as f:
            self.main_instructions= f.read()

    def update_synthetic_data(self, dml_content: str, test_definition: SLTestDefinition):
        self._load_prompts()
        prompt = self.main_instructions.format(dml_content=dml_content, test_definition=test_definition)
        response = self.llm_client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        return response.choices[0].message.content

    
    
            