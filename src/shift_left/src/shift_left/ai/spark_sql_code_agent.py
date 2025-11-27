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
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent, SqlTableDetection, FlinkSqlForRefinement, ErrorCategory

class SparkSqlFlinkDml(BaseModel):
    flink_dml_output: str

class SparkSqlFlinkDdl(BaseModel):
    flink_ddl_output: str
    key_name: str

class SqlRefinement(BaseModel):
    refined_ddl: str
    refined_dml: str
    explanation: str
    changes_made: List[str]


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
        print(f"Starting  Spark SQL to Flink SQL translation with {self.model_name}")
        logger.info(f"Starting translation with validation={validate}")

        # Reset validation history
        self.validation_history = []
        logger.info("\n0/ Cleaning SQL input by removing DROP TABLE statements and comments...\n")
        sql = self._clean_sql_input(sql)
        logger.info(f"Cleaned SQL input: {sql[:400]}...")
        logger.info(f"\n1/ Analyzing SQL input for multiple CREATE TABLE statements using: {self.model_name} \n")
        table_detection = self._table_detection_agent(sql)
        logger.info(f"Table detection result: {table_detection.model_dump_json()}")
        final_ddl = []
        final_dml = []
        if table_detection.has_multiple_tables:
            # Process multiple statements individually for better accuracy
            logger.info(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")
            for i, table_statement in enumerate(table_detection.table_statements):
                logger.info(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                # Translate individual statement
                ddl_sql, dml_sql = self._translator_agent(table_statement)
                logger.info(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql[:100]}..., DML: {dml_sql[:50] if dml_sql else 'empty'}...")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Validate and refine the translation
                ddl_sql, dml_sql = self._mandatory_validation_agent(ddl_sql, dml_sql)
                logger.info(f"Done with mandatory validation agent for statement {i+1}")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Collect non-empty results
                if ddl_sql.strip():
                    final_ddl.append(ddl_sql)
                if dml_sql and dml_sql.strip():
                    final_dml.append(dml_sql)
        else:
            # Step 1: Translate Spark SQL to Flink SQL
            print("\nStep 1: Processing single Spark SQL to Flink SQL...")
            ddl_sql, dml_sql = self._translator_agent(sql)
            print(f"Initial Migrated DML: {dml_sql[:300]}..." if len(dml_sql) > 300 else f"Initial DML: {dml_sql}")
            if not dml_sql or dml_sql.strip() == "":
                print("Translation failed - no output generated")
                return [""], [""]

            # Step 2: Generate DDL from DML if needed
            print("\nStep 2: Generating DDL from DML...")
            if not ddl_sql or ddl_sql.strip() == "":
                ddl_sql = self._ddl_generation(dml_sql)
                print(f"Initial Migrated DDL: {ddl_sql[:300]}..." if len(ddl_sql) > 300 else f"Initial DDL: {ddl_sql}")
            ddl_sql, dml_sql = self._mandatory_validation_agent(ddl_sql, dml_sql)
            self._snapshot_ddl_dml(table_name, ddl_sql, dml_sql)
            final_ddl = [ddl_sql]
            final_dml = [dml_sql]
        # Step 3: Validation (if enabled)
        if validate:
            print("3/ Start validating the flink SQLs using Confluent Cloud for Flink, do you want to continue? (y/n)")
            answer = input()
            if answer == "y":
                validated_ddls = []
                validated_dmls = []

                # Validate each DDL/DML pair against live environment
                for ddl, dml in zip(final_ddl, final_dml):
                    # Validate DDL first (tables must exist before queries)
                    ddl, validated = self._iterate_on_validation(ddl)
                    if validated:
                        ddl = self._process_syntax_validation(ddl)
                        if dml:
                            dml, validated = self._iterate_on_validation(dml)
                            if validated:
                                dml = self._process_syntax_validation(dml)
                    validated_dmls.append(dml)
                    validated_ddls.append(ddl)
                    self._snapshot_ddl_dml(table_name, ddl, dml)
                # Use validated results
                final_ddl = validated_ddls
                final_dml = validated_dmls

        return final_ddl, final_dml

    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("ddl_creation.txt")
        with fname.open("r") as f:
            self.ddl_creation_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("refinement.txt")
        with fname.open("r") as f:
            self.refinement_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("mandatory_validation.txt")
        with fname.open("r") as f:
            self.mandatory_validation_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt= f.read()


    def _translator_agent(self, sql: str) -> str:
        """Translator agent with error handling"""
        translator_prompt_template = "spark_sql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]

        try:
            response= self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=SparkSqlFlinkDml,
                messages=messages
            )
            obj_response = response.choices[0].message
            logger.info(f"Translation response: {obj_response.parsed}")
            print(f"Translation response: {obj_response.parsed}")

            if obj_response.parsed:
                return obj_response.parsed.flink_dml_output
            else:
                return "", ""
        except Exception as e:
            logger.error(f"Translation failed: {str(e)}")
            print(f"Translation failed: {str(e)}")
            return "", ""

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

    def _refinement_agent(self, ddl_sql: str, dml_sql: str,
                         error_message: str, history: List[Dict]) -> Tuple[str, str]:
        """Enhanced refinement agent for error correction"""
        try:
            refinement_prompt = self.refinement_system_prompt.format(
                error_message=error_message,
                history=json.dumps(history, indent=2),
                ddl_sql=ddl_sql,
                dml_sql=dml_sql
            )

            messages = [
                {"role": "system", "content": refinement_prompt},
                {"role": "user", "content": f"Please fix the following Flink SQL error:\n\nError: {error_message}\n\nDDL:\n{ddl_sql}\n\nDML:\n{dml_sql}"}
            ]

            response = self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=SqlRefinement,
                messages=messages
            )

            obj_response = response.choices[0].message
            logger.info(f"Refinement response: {obj_response.parsed}")
            print(f"Refinement response: {obj_response.parsed}")

            if obj_response.parsed:
                print(f"Changes made: {obj_response.parsed.changes_made}")
                print(f"Explanation: {obj_response.parsed.explanation}")
                return obj_response.parsed.refined_ddl, obj_response.parsed.refined_dml
            else:
                logger.error("No parsed response from refinement agent")
                return ddl_sql, dml_sql

        except Exception as e:
            logger.error(f"Refinement agent failed: {str(e)}")
            print(f"Refinement agent failed: {str(e)}")
            return ddl_sql, dml_sql

    def _validate_with_confluent_cloud(self, ddl_sql: str, dml_sql: str) -> Tuple[bool, str]:
        """Validate SQL statements using Confluent Cloud Flink"""
        # First validate DDL if provided
        if ddl_sql.strip():
            ddl_valid, ddl_error = self._validate_flink_sql_on_cc(ddl_sql)
            if not ddl_valid:
                return False, f"DDL Error: {ddl_error}"

        # Then validate DML
        if dml_sql.strip():
            dml_valid, dml_error = self._validate_flink_sql_on_cc(dml_sql)
            if not dml_valid:
                return False, f"DML Error: {dml_error}"

        return True, "Validation successful"

    def _mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        """Validation agent with iterative refinement and human in the loop"""
        print("Starting enhanced validation process...")
        logger.info("Starting enhanced validation with Confluent Cloud")

        # Pre-validation syntax check
        syntax_valid, syntax_error = self._pre_validate_syntax(ddl_sql, dml_sql)
        if not syntax_valid:
            print(f"Pre-validation failed: {syntax_error}")
            logger.warning(f"Pre-validation failed: {syntax_error}")
            # Try to fix basic syntax issues
            ddl_sql, dml_sql = self._refinement_agent(ddl_sql, dml_sql, f"Syntax validation failed: {syntax_error}", [])

        # Iterative validation with Confluent Cloud
        max_iterations = 3
        current_ddl, current_dml = ddl_sql, dml_sql

        for iteration in range(max_iterations):
            print(f"\n--- Validation Iteration {iteration + 1} ---")

            # Validate with Flink Engine
            is_valid, error_message = self._validate_with_confluent_cloud(current_ddl, current_dml)

            # Track validation history
            validation_entry = {
                "iteration": iteration + 1,
                "ddl": current_ddl,
                "dml": current_dml,
                "is_valid": is_valid,
                "error": error_message if not is_valid else None,
                "error_category": self._categorize_error(error_message).value if not is_valid else None
            }
            self.validation_history.append(validation_entry)

            if is_valid:
                print(f"✅ SQL validation successful after {iteration + 1} iterations")
                logger.info(f"SQL validation successful after {iteration + 1} iterations")
                return current_ddl, current_dml

            print(f"❌ Validation failed: {error_message}")
            logger.warning(f"Validation failed: {error_message}")

            # Use refinement agent to fix the error
            if iteration < max_iterations - 1:  # Don't refine on last iteration
                print("Attempting to fix with refinement agent...")
                refined_ddl, refined_dml = self._refinement_agent(
                    current_ddl,
                    current_dml,
                    error_message,
                    self.validation_history
                )
                current_ddl, current_dml = refined_ddl, refined_dml

        print(f"⚠️  Could not fix SQL after {max_iterations} iterations. Returning last attempt.")
        logger.warning(f"Could not fix SQL after {max_iterations} iterations")
        return current_ddl, current_dml

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




    def get_validation_history(self) -> List[Dict]:
        """Return the validation history for debugging purposes"""
        return self.validation_history
