"""
Copyright 2024-2025 Confluent, Inc.

Spark SQL to Flink SQL Translation Agent

An enhanced agentic flow to translate Spark SQL to Flink SQL with
validation and error correction.

The translation process includes multiple validation
steps and can handle both single and multiple table/stream definitions.

"""

from pydantic import BaseModel
from typing import Tuple, List, Dict
import json
import importlib.resources


from shift_left.core.utils.app_config import logger
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent, SqlTableDetection, FlinkSqlForRefinement, ErrorCategory, FlinkSql
from shift_left.core.utils.sql_parser import SQLparser

class SparkSqlFlinkDdl(BaseModel):
    flink_ddl_output: str
    key_name: str

class SparkToFlinkSqlAgent(TranslatorToFlinkSqlAgent):
    def __init__(self):
        super().__init__()
        self._load_prompts()

    # -------------------------
    # Public API
    # -------------------------
    def translate_to_flink_sqls(self,
                    table_name: str,
                    sql: str,
                    validate: bool = True
    ) -> Tuple[List[str], List[str]]:
        """Translation with validation enabled by default"""
        logger.info("\n0/ Cleaning SQL input by removing DROP TABLE statements and comments...\n")
        print("\n0/ Cleaning SQL input by removing DROP TABLE statements and comments...\n")

        sql = self._clean_sql_input(sql)
        logger.info(f"Cleaned SQL input: {sql[:400]}...")
        # Reset validation history
        self.validation_history = []

        logger.info(f"\n1/ Analyzing SQL input for multiple CREATE TABLE statements using: {self.model_name} and ccloud: {validate}\n")
        table_detection = self._detect_multitable_with_agent(sql)
        logger.info(f"Table detection result: {table_detection.model_dump_json()}")
        final_ddl = []
        final_dml = []
        logger.info(f"Starting translation using {self.model_name} with validation={validate}")
        print(f"-"*40)
        print(f"Starting translation using {self.model_name} with validation={validate}")
        if table_detection.has_multiple_tables:
            # Process multiple statements individually for better accuracy
            logger.info(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")
            for i, table_statement in enumerate(table_detection.table_statements):
                logger.info(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                # Translate individual statement
                ddl_sql, dml_sql = self._do_translation_with_agent(table_statement)
                logger.info(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql[:100]}..., DML: {dml_sql[:50] if dml_sql else 'empty'}...")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Validate and refine the translation
                ddl_sql, dml_sql = self._run_mandatory_validation_agent(ddl_sql, dml_sql)
                logger.info(f"Done with mandatory validation agent for statement {i+1}")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Collect non-empty results
                if ddl_sql.strip():
                    final_ddl.append(ddl_sql)
                if dml_sql and dml_sql.strip():
                    final_dml.append(dml_sql)
        else:
            print("\n2/: Processing single Spark SQL to Flink SQL...")
            ddl_sql, dml_sql = self._do_translation_with_agent(sql)
            print(f"Initial Migrated DDL: {ddl_sql[:300]}... and DML: {dml_sql[:300]}...")
            other_ddl=self._create_other_ddl_from_dml_if_needed(ddl_sql, dml_sql)
            if other_ddl:
                final_ddl.append(other_ddl)
            ddl_sql, dml_sql = self._run_mandatory_validation_agent(ddl_sql, dml_sql)
            self._snapshot_ddl_dml(table_name, ddl_sql, dml_sql)
            final_ddl.append(ddl_sql)
            final_dml = [dml_sql]
        # Last Step: Validation (if enabled)
        if validate:
            final_ddl, final_dml = self._validate_ddl_dml_on_cc(final_ddl, final_dml)
        return final_ddl, final_dml

    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("ddl_creation.txt")
        with fname.open("r") as f:
            self.ddl_creation_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt= f.read()



    def _categorize_error(self, error_message: str) -> ErrorCategory:
        """Categorize the error based on error message patterns"""
        error_lower = error_message.lower()

        if any(keyword in error_lower for keyword in ['syntax', 'parsing', 'parse', 'token']):
            return ErrorCategory.SYNTAX_ERROR
        elif any(keyword in error_lower for keyword in ['function', 'operator', 'not supported']):
            return ErrorCategory.FUNCTION_INCOMPATIBILITY
        elif any(keyword in error_lower for keyword in ['type', 'cast', 'conversion']):
            return ErrorCategory.TYPE_MISMATCH
        elif any(keyword in error_lower for keyword in ['watermark', 'timestamp', 'event time']):
            return ErrorCategory.WATERMARK_ISSUE
        elif any(keyword in error_lower for keyword in ['connector', 'properties', 'table']):
            return ErrorCategory.CONNECTOR_ISSUE
        elif any(keyword in error_lower for keyword in ['semantic', 'reference', 'not found']):
            return ErrorCategory.SEMANTIC_ERROR
        else:
            return ErrorCategory.UNKNOWN

    def _pre_validate_syntax(self, ddl_sql: str, dml_sql: str) -> Tuple[bool, str]:
        """Basic syntax validation before sending to Confluent Cloud"""
        issues = []

        # Check for basic SQL structure
        if dml_sql and not dml_sql.strip().upper().startswith(('INSERT', 'SELECT', 'WITH')):
            issues.append("DML must start with INSERT, SELECT, or WITH")

        if ddl_sql and not ddl_sql.strip().upper().startswith('CREATE'):
            issues.append("DDL must start with CREATE")

        # Check for balanced parentheses
        for sql_type, sql in [("DDL", ddl_sql), ("DML", dml_sql)]:
            if sql:
                open_parens = sql.count('(')
                close_parens = sql.count(')')
                if open_parens != close_parens:
                    issues.append(f"{sql_type} has unbalanced parentheses")

        # Check for common Spark-specific functions that weren't translated
        spark_functions = ['current_timestamp()', 'split_part', 'surrogate_key']
        for sql_type, sql in [("DDL", ddl_sql), ("DML", dml_sql)]:
            if sql:
                for func in spark_functions:
                    if func in sql.lower():
                        issues.append(f"{sql_type} contains untranslated Spark function: {func}")

        if issues:
            return False, "; ".join(issues)
        return True, ""

    def _ddl_generation(self, dml_sql: str) -> str:
        """DDL generationfrom the dml content """
        if not dml_sql.strip():
            logger.warning("No DML provided for DDL generation")
            return ""

        translator_prompt_template = "flink_sql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.ddl_creation_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=dml_sql)}
        ]

        try:
            response= self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=SparkSqlFlinkDdl,
                messages=messages
            )
            obj_response = response.choices[0].message
            logger.info(f"DDL generation response: {obj_response.parsed}")
            print(f"DDL generation response: {obj_response.parsed}")

            if obj_response.parsed:
                return obj_response.parsed.flink_ddl_output
            else:
                return ""
        except Exception as e:
            logger.error(f"DDL generation failed: {str(e)}")
            print(f"DDL generation failed: {str(e)}")
            return ""

    def _create_other_ddl_from_dml_if_needed(self, ddl_sql: str, dml_sql: str) -> str:
        """Create DDL from DML if needed"""
        other_ddl = ""
        if dml_sql and len(dml_sql.strip())> 10:
            if (not ddl_sql or ddl_sql.strip() == ""):
                print("\n3/ Generating DDL from DML...")
                other_ddl = self._ddl_generation(dml_sql)
            else:
                sql_parser = SQLparser()
                table_name_in_insert = sql_parser.extract_table_name_from_insert_into_statement(dml_sql)
                table_name_in_ddl = sql_parser.extract_table_name_from_create_statement(ddl_sql)
                if table_name_in_insert != table_name_in_ddl:
                    print("\n3/ Generating DDL from DML...")
                    other_ddl = self._ddl_generation(dml_sql)
                else:
                    print("\n3/ No need to create DDL from DML...")
                    other_ddl= ddl_sql
        print(f"--> Extracted DDL: {other_ddl}")
        return other_ddl
