"""
Copyright 2024-2025 Confluent, Inc.

An enhanced agentic flow to translate Spark SQL to Flink SQL with validation and error correction.
"""

from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional
import os
import re
import json
import importlib.resources 
from enum import Enum

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement
from shift_left.core.utils.llm_code_agent_base import AnySqlToFlinkSqlAgent

class SparkSqlFlinkSql(BaseModel):
    flink_ddl_output: str
    flink_dml_output: str

class SparkSqlFlinkDdl(BaseModel):
    flink_ddl_output: str

class SqlRefinement(BaseModel):
    refined_ddl: str
    refined_dml: str
    explanation: str
    changes_made: List[str]

class ErrorCategory(Enum):
    SYNTAX_ERROR = "syntax_error"
    FUNCTION_INCOMPATIBILITY = "function_incompatibility"
    TYPE_MISMATCH = "type_mismatch" 
    WATERMARK_ISSUE = "watermark_issue"
    CONNECTOR_ISSUE = "connector_issue"
    SEMANTIC_ERROR = "semantic_error"
    UNKNOWN = "unknown"


class SparkToFlinkSqlAgent(AnySqlToFlinkSqlAgent):
    def __init__(self):
        super().__init__()
        self.refinement_system_prompt = ""
        self.validation_history: List[Dict] = []
        
    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.core.utils.prompts.spark_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.spark_fsql").joinpath("ddl_creation.txt")
        with fname.open("r") as f:
            self.ddl_creation_system_prompt= f.read()
        
        # Load or create refinement prompt
        self._load_refinement_prompt()

    def _load_refinement_prompt(self):
        """Load or create the refinement prompt for error correction"""
        self.refinement_system_prompt = """You are an expert Apache Flink SQL error correction agent. Your task is to fix Flink SQL errors based on error messages from Confluent Cloud Flink.

## Your Role:
- Analyze Flink SQL errors and categorize them
- Apply targeted fixes based on error patterns
- Ensure streaming semantics are preserved
- Maintain data correctness while fixing syntax/semantic issues

## Common Error Patterns and Fixes:

### Syntax Errors:
- Missing semicolons or incorrect SQL syntax
- Invalid function calls or parameter counts
- Incorrect table/column references

### Function Incompatibility:
- Replace Spark-specific functions with Flink equivalents
- Fix function parameter ordering and types
- Handle unsupported Spark functions

### Type Mismatches:
- Convert between compatible types using CAST/TRY_CAST
- Fix precision/scale issues with DECIMAL types
- Handle NULL/NOT NULL constraints

### Watermark Issues:
- Add proper WATERMARK definitions for event-time processing
- Fix timestamp column references
- Correct watermark delay specifications

### Connector Issues:
- Fix table property syntax
- Correct connector configurations
- Handle missing required properties

## Output Requirements:
- Provide refined DDL and DML statements
- Explain what changes were made and why
- List specific changes in bullet points
- Ensure the fix addresses the root cause

Current error to fix: {error_message}
Previous attempts: {history}
Original DDL: {ddl_sql}
Original DML: {dml_sql}"""

    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        """Enhanced translator agent with better error handling"""
        translator_prompt_template = "spark_sql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        
        try:
            response= self.llm_client.chat.completions.parse(
                model=self.model_name, 
                response_format=SparkSqlFlinkSql,
                messages=messages
            )
            obj_response = response.choices[0].message
            logger.info(f"Translation response: {obj_response.parsed}")
            print(f"Translation response: {obj_response.parsed}")
            
            if obj_response.parsed:
                return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
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
        """Enhanced validation agent with iterative refinement"""
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
            
            # Validate with Confluent Cloud
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
        """Enhanced DDL generation with better error handling"""
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

    def translate_to_flink_sql(self, sql: str, validate: bool = True) -> Tuple[str, str]:
        """Enhanced translation with validation enabled by default"""
        print(f"🚀 Starting enhanced Spark SQL to Flink SQL translation with {self.model_name}")
        logger.info(f"Starting translation with validation={validate}")
        
        # Reset validation history
        self.validation_history = []
        
        # Step 1: Translate Spark SQL to Flink SQL
        print("\n📝 Step 1: Translating Spark SQL to Flink SQL...")
        ddl_sql, dml_sql = self._translator_agent(sql)
        print(f"Initial DDL: {ddl_sql[:100]}..." if len(ddl_sql) > 100 else f"Initial DDL: {ddl_sql}")    
        print(f"Initial DML: {dml_sql[:100]}..." if len(dml_sql) > 100 else f"Initial DML: {dml_sql}")
        
        if not ddl_sql and not dml_sql:
            print("❌ Translation failed - no output generated")
            return "", ""
        
        # Step 2: Generate DDL from DML if needed
        if not ddl_sql and dml_sql:
            print("\n🏗️  Step 2: Generating DDL from DML...")
            ddl_sql = self._ddl_generation(dml_sql)
        
        # Step 3: Validation (if enabled)
        if validate:
            print("\n🔍 Step 3: Validating with agentic refinement...")
            final_ddl, final_dml = self._mandatory_validation_agent(ddl_sql, dml_sql)
        else:
            print("\n⏭️  Step 3: Skipping validation (disabled)")
            final_ddl, final_dml = ddl_sql, dml_sql
        
        print(f"\n✅ Translation complete!")
        print(f"Final DDL length: {len(final_ddl)} characters")
        print(f"Final DML length: {len(final_dml)} characters")
        
        logger.info("Translation completed successfully")
        return final_ddl, final_dml

    def get_validation_history(self) -> List[Dict]:
        """Return the validation history for debugging purposes"""
        return self.validation_history
