"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import re
from pydantic import BaseModel
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
import json
from importlib import import_module
from enum import Enum
from typing import Tuple, List, Optional, Dict
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
import importlib.resources
from shift_left.ai.rag import rag_enabled


def _strip_markdown_json(text: str) -> str:
    """
    Remove markdown code fences from JSON so Pydantic can parse.
    Handles ```json ... ``` or ``` ... ``` wrapping.
    """
    if not text or not text.strip():
        return text
    s = text.strip()
    if s.startswith("```"):
        # Remove opening fence (```json or ```)
        first_newline = s.find("\n")
        if first_newline != -1:
            s = s[first_newline + 1 :]
        else:
            s = s[3:]
            if s.startswith("json"):
                s = s[4:]
            s = s.lstrip()
        # Remove closing fence
        if s.endswith("```"):
            s = s[: -3].rstrip()
    return s


def _split_sql_create_statements(sql: str) -> List[str]:
    """
    Split KSQL script into separate CREATE TABLE and CREATE STREAM statements (deterministic).
    Statement boundaries: each CREATE runs until the next semicolon or the next CREATE keyword.
    """
    if not sql or not sql.strip():
        return []
    pattern = re.compile(r"\bCREATE\s+(?:TABLE|STREAM)\b", re.IGNORECASE)
    starts = [m.start() for m in pattern.finditer(sql)]
    if not starts:
        return []
    statements: List[str] = []
    for i, start in enumerate(starts):
        end: int
        next_create = starts[i + 1] if i + 1 < len(starts) else len(sql)
        semi = sql.find(";", start)
        if semi != -1 and semi < next_create:
            end = semi + 1
        else:
            end = next_create
        stmt = sql[start:end].strip()
        if stmt:
            statements.append(stmt)
    return statements


class SqlTableDetection(BaseModel):
    """
    Model for detecting and splitting multiple CREATE TABLE/STREAM statements in KSQL input.

    This model helps identify when the input contains multiple table or stream definitions
    that should be processed separately for better translation accuracy.

    Attributes:
        has_multiple_tables: Whether the input contains multiple CREATE statements
        table_statements: List of individual CREATE statements if multiple were found
        description: Human-readable description of the detection result
    """
    has_multiple_tables: bool
    table_statements: List[str]
    description: str

class FlinkSql(BaseModel):
    """
    Model for Flink SQL validation and refinement process.

    Used during the mandatory validation step to ensure the generated Flink SQL
    is syntactically correct and follows best practices.

    Attributes:
        ddl_sql_input: Input DDL statements for validation (optional)
        dml_sql_input: Input DML statements for validation (optional)
        flink_ddl_output: Validated and potentially corrected DDL output (optional)
        flink_dml_output: Validated and potentially corrected DML output (optional)
    """
    ddl_sql_input: Optional[str] = None
    dml_sql_input: Optional[str] = None
    flink_ddl_output: Optional[str] = None
    flink_dml_output: Optional[str] = None


class SourceSql2FlinkSql(BaseModel):
    """
    Structured response model for KSQL to Flink SQL translation.

    This model captures the LLM's translation output, separating DDL (Data Definition Language)
    and DML (Data Manipulation Language) statements for better organization.

    Attributes:
        sql_input: The original SQL statement (optional; LLM may omit it)
        flink_ddl_output: Generated Flink SQL DDL statements (CREATE TABLE, etc.)
        flink_dml_output: Generated Flink SQL DML statements (INSERT, SELECT, etc.)
    """
    sql_input: Optional[str] = ""
    flink_ddl_output: Optional[str] = ""
    flink_dml_output: Optional[str] = ""

class FlinkSqlForRefinement(BaseModel):
    """
    Model for SQL refinement when errors are encountered during validation.

    This model is used when the initial translation fails validation and needs
    to be refined based on error feedback from the Flink execution environment.

    Attributes:
        sql_input: The SQL statement that failed validation
        error_message: Error message received from Flink
        flink_output: Refined SQL statement that should resolve the error
    """
    sql_input: Optional[str] = None
    error_message: Optional[str] = None
    flink_output: Optional[str] = None

class ErrorCategory(Enum):
    SYNTAX_ERROR = "syntax_error"
    FUNCTION_INCOMPATIBILITY = "function_incompatibility"
    TYPE_MISMATCH = "type_mismatch"
    WATERMARK_ISSUE = "watermark_issue"
    CONNECTOR_ISSUE = "connector_issue"
    SEMANTIC_ERROR = "semantic_error"
    UNKNOWN = "unknown"
class TranslatorToFlinkSqlAgent():
    """
    Translator to Flink SQL agent. Abstract class.

    """
    def __init__(self):
        self.default_model_name="qwen3-coder-30b-a3b-instruct-mlx-4bit"
        self.table_detection_system_prompt = ""
        self.model_name=os.getenv("SL_LLM_MODEL",self.default_model_name)
        self.llm_base_url=os.getenv("SL_LLM_BASE_URL","http://localhost:1337/v1")
        self.llm_api_key=os.getenv("SL_LLM_API_KEY","no_llm_key")
        print(f"Using {self.model_name} with {self.llm_base_url} and {self.llm_api_key[:25]}...", flush=True)
        self.llm_client = OpenAI(api_key=self.llm_api_key, base_url=self.llm_base_url)
        self.validation_history: List[Dict] = []
        self.translator_system_prompt = ""
        self.use_rag_for_translation = False
        fname = importlib.resources.files("shift_left.ai.prompts.common").joinpath("refinement.txt")
        with fname.open("r") as f:
            self.refinement_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.common").joinpath("mandatory_validation.txt")
        with fname.open("r") as f:
            self.mandatory_validation_system_prompt= f.read()
        self._load_prompts()
        try:
            self.use_rag_for_translation = rag_enabled()
        except ImportError:
            self.use_rag_for_translation = False



    def translate_to_flink_sqls(self,
                table_name: str,
                sql: str,
                validate: bool = False
    ) -> Tuple[List[str], List[str]]:
        """
        Main entry point for source SQL to Flink SQL translation workflow.

        This method orchestrates the complete translation pipeline:
        1. Input cleaning and preprocessing
        2. LLM based Multiple table detection and splitting
        3. LLM-based translation for each statement
        4. Mandatory validation and syntax checking
        5. Optional live validation against Confluent Cloud for Apache Flink
        6. Iterative refinement based on execution feedback

        Args:
            table_name (str): The name of the table to translate
            sql (str): The SQL input to translate (can contain multiple statements)
            validate (bool, optional): Whether to perform live validation against Flink.
                                     Defaults to False. When True, requires user confirmation.

        Returns:
            Tuple[List[str], List[str]]: A tuple containing:
                - List of translated DDL statements
                - List of translated DML statements

        Note:
            When validate=True, the method will prompt for user confirmation before
            executing statements against the live Flink environment.
        """

        logger.info("\n0/ Cleaning KSQL input by removing DROP TABLE statements and comments...\n")
        print("\n0/ Cleaning SQL input by removing DROP TABLE statements and comments...\n", flush=True)

        cleaned_sql = self._clean_sql_input(sql)
        logger.info(f"Cleaned source SQL input: {cleaned_sql[:400]}...")

        logger.info(f"\n1/ Analyzing source SQL input for multiple CREATE TABLE statements using: {self.model_name} \n")
        print(f"\n1/ Analyzing source SQL input for multiple CREATE TABLE statements using: {self.model_name} \n", flush=True)

        table_detection = self._detect_multitable_with_agent(cleaned_sql)
        logger.info(f"Table detection result: {table_detection.model_dump_json()}")
        final_ddl = []
        final_dml = []
        print(f"Starting translation using {self.model_name} with cc-validation={validate}", flush=True)
        print(f"-"*40, flush=True)
        if table_detection.has_multiple_tables:
            # Process multiple statements individually for better accuracy
            logger.info(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")
            for i, table_statement in enumerate(table_detection.table_statements):
                logger.info(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:150]}...")
                print(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:150]}...", flush=True)
                # Translate individual statement
                ddl_sql, dml_sql = self._do_translation_with_agent(table_statement)
                logger.info(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql}..., DML: {dml_sql if dml_sql else 'empty'}...")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Validate and refine the translation
                ddl_sql, dml_sql = self._run_mandatory_validation_agent(ddl_sql, dml_sql)
                logger.info(f"Done with mandatory validation agent for statement {i+1}")
                self._snapshot_ddl_dml(table_name + "_v_" + str(i), ddl_sql, dml_sql)
                # Collect non-empty results
                if ddl_sql and ddl_sql.strip():
                    final_ddl.append(ddl_sql)
                if dml_sql and dml_sql.strip():
                    final_dml.append(dml_sql)
        else:
            # Process as single statement
            logger.info("2/ Processing single source SQL statement...")
            print("\n2/: Processing single source SQL to Flink SQL...", flush=True)
            ddl_sql, dml_sql = self._do_translation_with_agent(cleaned_sql)

            self._snapshot_ddl_dml(table_name, ddl_sql, dml_sql)
            logger.info(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}\n3/ Start mandatory validation agent...")
            ddl, dml = self._run_mandatory_validation_agent(ddl_sql, dml_sql)
            self._snapshot_ddl_dml(table_name, ddl, dml)
            logger.info(f"Done with mandatory validation agent, updated flink DDL sql is:\n {ddl}\nand DML: {dml if dml else 'empty'}")
            final_ddl = [ddl]
            final_dml = [dml]
        # Optional live validation against Confluent Cloud for Apache Flink
        if validate:
            final_ddl, final_dml = self._validate_ddl_dml_on_cc(final_ddl, final_dml)
        return final_ddl, final_dml

    # ------------------------------------------------------------
    # Abstract Public methods to be implemented by subclasses
    # ------------------------------------------------------------
    # Abstract Private methods to be implemented by subclasses
    def _load_prompts(self):
        print("To complement", flush=True)
        pass
    # ------------------------------------------------------------
    # Common private methods
    # ------------------------------------------------------------
    def _clean_sql_input(self, sql: str) -> str:
        """
        Clean SQL input by removing problematic statements and comments.

        This preprocessing step removes:
        - DROP TABLE and DROP STREAM statements (not needed for translation)
        - Comment lines starting with '--' (can confuse the LLM)
        - Preserves empty lines for formatting consistency

        Args:
            sql (str): The raw SQL string to clean

        Returns:
            str: Cleaned SQL string with unwanted statements and comments removed
        """
        lines = sql.split('\n')
        cleaned_lines = []

        for line in lines:
            stripped_line = line.strip()

            # Preserve empty lines for formatting
            if not stripped_line:
                cleaned_lines.append(stripped_line)
                continue

            # Remove '--' comments
            if stripped_line.startswith('--'):
                continue

            # Remove lines inside block comments /* ... */
            if not hasattr(self, "_in_block_comment"):
                self._in_block_comment = False
            if '/*' in stripped_line and '*/' in stripped_line:
                # Single-line block comment
                continue
            elif '/*' in stripped_line:
                self._in_block_comment = True
                continue
            elif '*/' in stripped_line:
                self._in_block_comment = False
                continue
            if getattr(self, "_in_block_comment", False):
                continue

            # These are not needed for translation and can confuse the LLM
            if stripped_line.upper().startswith('DROP TABLE'):
                continue

            if stripped_line.upper().startswith('DROP STREAM'):
                continue

            # Keep all other lines including the original indentation
            cleaned_lines.append(stripped_line)

        return '\n'.join(cleaned_lines)

    def _clean_output(self,output: str) -> str:
        """
        Cleans the output string by replacing unwanted patterns like '\\\\n', '\\n', etc.
        Add additional patterns as necessary.
        """
        if not output:
            return output
        cleaned = output.replace('\\\\n', ' ')
        cleaned = cleaned.replace('\\n', ' ')
        cleaned = cleaned.replace('\n', ' ')
        cleaned = cleaned.replace('\\', ' ')
        return cleaned

    def _build_rag_examples_block(self, sql: str) -> str:
        """
        If RAG is enabled and a store is available, retrieve similar (ksql, Flink) examples
        and return a prompt block; otherwise return empty string.
        """
        if not getattr(self, "use_rag_for_translation", False):
            return ""
        try:
            from shift_left.ai.rag import get_rag_store, rag_enabled, rag_top_k
            if not rag_enabled():
                return ""
            store = get_rag_store()
            if store is None:
                return ""
            pairs = store.search(sql, top_k=rag_top_k())
            if not pairs:
                return ""
            lines = ["\n\n## Retrieved similar examples (use as reference for style and patterns):\n"]
            for i, p in enumerate(pairs, 1):
                lines.append(f"Example {i}:")
                lines.append(f"KSQL:\n{p.ksql_text}")
                if p.flink_ddl:
                    lines.append(f"Flink DDL:\n{p.flink_ddl}")
                if p.flink_dml:
                    lines.append(f"Flink DML:\n{p.flink_dml}")
                lines.append("")
            return "\n".join(lines)
        except ImportError:
            return ""

    def _do_translation_with_agent(self, sql: str) -> Tuple[str | None, str | None ]:
        """
        Translate a single KSQL or Spark SQL statement to Flink SQL using LLM.

        This is the core translation agent that converts KSQL syntax to equivalent
        Flink SQL statements. It separates the output into DDL and DML components
        for better organization and downstream processing.

        Args:
            sql (str): A source SQL statement or group of related statements

        Returns:
            Tuple[str, str]: A tuple containing:
                - flink_ddl_output: Flink SQL DDL statements (CREATE TABLE, etc.)
                - flink_dml_output: Flink SQL DML statements (INSERT, SELECT, etc.)
        """
        system_content = self.translator_system_prompt + self._build_rag_examples_block(sql)
        translator_prompt_template = "sql_input: {sql_input}"
        messages: list[ChatCompletionMessageParam] = [
            {"role": "system", "content": system_content},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        logger.info(f"Messages: {messages}")
        try:
            # Use create() then parse so we can strip markdown code fences (e.g. ```json ... ```)
            response = self.llm_client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=0.2,
                max_completion_tokens=10000,
            )
            content = (response.choices[0].message.content or "").strip()
            if not content:
                return sql, ""
            content = _strip_markdown_json(content)
            parsed = SourceSql2FlinkSql.model_validate_json(content)
            logger.info(f"\nTranslation Response: {parsed}")
            print(f"\nTranslation Response: {parsed}", flush=True)
            ddl_output = self._clean_output(parsed.flink_ddl_output or "")
            dml_output = self._clean_output(parsed.flink_dml_output or "")
            return ddl_output, dml_output
        except Exception as e:
            logger.error(f"Error in translator agent: {e}")
            return sql, ""

    def _detect_multitable_with_agent(self, sql: str) -> SqlTableDetection:
        """
        Analyze SQL input to detect multiple CREATE TABLE/STREAM statements.

        Uses deterministic split first (no LLM call). Processing multiple statements
        individually often yields better translation results than processing them together.

        Args:
            sql (str): The cleaned SQL input to analyze

        Returns:
            SqlTableDetection: Analysis result containing:
                - Whether multiple tables were detected
                - List of individual statements if multiple were found
                - Description of the detection result
        """
        # Deterministic first: no agent call when split is sufficient
        split_list = _split_sql_create_statements(sql)
        n = len(split_list)
        if n > 1:
            logger.info(f"Table detection (deterministic): {n} CREATE statements")
            return SqlTableDetection(
                has_multiple_tables=True,
                table_statements=split_list,
                description=f"Split by CREATE boundaries: {n} statements"
            )
        if n == 1:
            return SqlTableDetection(
                has_multiple_tables=False,
                table_statements=split_list,
                description="Single CREATE statement"
            )
        # No CREATE TABLE/STREAM found: treat as single script (downstream uses full sql)
        return SqlTableDetection(
            has_multiple_tables=False,
            table_statements=[sql],
            description="No CREATE statements found, treating as single script"
        )

    def _run_mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        """
        Perform mandatory validation and correction of generated Flink SQL.

        This agent reviews the initially translated SQL for:
        - Syntax correctness according to Flink SQL standards
        - Best practices and common patterns
        - Structural improvements and optimizations
        - Compliance with Confluent Cloud for Apache Flink requirements

        Args:
            ddl_sql (str): The DDL statements to validate and potentially correct
            dml_sql (str): The DML statements to validate and potentially correct

        Returns:
            Tuple[str, str]: A tuple containing:
                - Validated and potentially corrected DDL statements
                - Validated and potentially corrected DML statements
        """
        logger.info(f"Starting mandatory validation agent with {self.model_name}")
        print(f"Starting mandatory validation agent with {self.model_name}", flush=True)
        syntax_checker_prompt_template = "ddl_sql_input: {ddl_sql_input}\ndml_sql_input: {dml_sql_input}"
        messages: list[ChatCompletionMessageParam] = [
            {"role": "system", "content": self.mandatory_validation_system_prompt},
            {"role": "user", "content": syntax_checker_prompt_template.format(ddl_sql_input=ddl_sql, dml_sql_input=dml_sql)}
        ]
        logger.info(f"Mandatory validation messages: {messages}")
        try:
            # Use structured output to maintain DDL/DML separation
            response= self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=FlinkSql,
                messages=messages
            )

            obj_response = response.choices[0].message
            if obj_response.parsed:
                logger.info(f"Mandatory validation response: {obj_response.parsed}")
                print(f"Mandatory validation response: {obj_response.parsed}", flush=True)
                return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
            else:
                # Return original inputs if validation fails
                return ddl_sql, dml_sql
        except Exception as e:
            logger.error(f"Error in mandatory validation agent: {e}")
            return ddl_sql, dml_sql

    def _validate_flink_sql_on_cc(self, sql_to_validate: str) -> Tuple[bool, str]:
        config = get_config()
        compute_pool_id = config.get('flink').get('compute_pool_id')
        statement = None
        if sql_to_validate:
            statement_name = "syntax-check"
            delete_statement_if_exists(statement_name)
            statement = post_flink_statement(compute_pool_id, statement_name, sql_to_validate)
            print(f"CC Flink Statement: {statement}", flush=True)
            logger.info(f"CC Flink Statement: {statement}")
            if statement and statement.status:
                if statement.status.phase in ["RUNNING", "COMPLETED"]:
                    # Stop the statement as it is not needed anymore
                    delete_statement_if_exists(statement_name)
                    return True, statement.status.detail
                else:
                    return False, statement.status.detail
            else:
                return False, "No statement found"
        else:
            return False, "No sql to validate"


    def _validate_ddl_dml_on_cc(self, ddls: List[str], dmls: List[str]) -> Tuple[List[str], List[str]]:
        """
        Validate DDL and DML on Confluent Cloud for Apache Flink.
        """
        print("\4/ Start validating the flink SQLs using Confluent Cloud for Flink, do you want to continue? (y/n)", flush=True)
        logger.info(f"Validating {len(ddls)} DDLs and {len(dmls)} DMLs on Confluent Cloud for Flink")
        answer = input()
        if answer == "y":
            validated_ddls = []
            validated_dmls = []

            # Validate each DDL/DML pair against live environment
            for ddl, dml in zip(ddls, dmls):
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
            return validated_ddls, validated_dmls
        else:
            return ddls, dmls

    def _run_refinement_agent(self, sql: str,
                         history: str,
                         error_message: str,) -> str:
        """
        Refine SQL statements based on execution errors from Flink environment.

        When SQL statements fail during live validation against Confluent Cloud for Apache Flink,
        this agent analyzes the error message and conversation history to generate
        corrected SQL that should resolve the specific issues encountered.

        Args:
            sql (str): The SQL statement that failed validation
            history (str): Conversation history and previous attempts for context
            error_message (str): Specific error message from Flink execution

        Returns:
            str: Refined SQL statement that addresses the reported error
        """
        try:
            refinement_prompt_template = "flink_sql_input: {sql_input}\nhistory of the conversation: {history}\nreported error: {error_message}"

            messages: list[ChatCompletionMessageParam] = [
                {"role": "system", "content": self.refinement_system_prompt},
                {"role": "user", "content": refinement_prompt_template.format(sql_input=sql, history=history, error_message=error_message)}
            ]

            response = self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=FlinkSqlForRefinement,
                messages=messages
            )

            obj_response = response.choices[0].message
            logger.info(f"Refinement response: {obj_response.parsed}")
            print(f"Refinement response: {obj_response.parsed}", flush=True)

            if obj_response.parsed:
                return obj_response.parsed.flink_output
            else:
                logger.error("No parsed response from refinement agent")
                return sql

        except Exception as e:
            logger.error(f"Refinement agent failed: {str(e)}")
            print(f"Refinement agent failed: {str(e)}", flush=True)
            return sql

    def _iterate_on_validation(self, translated_sql: str) -> Tuple[str, bool]:
        """
        Deploy the sql on Confluent Cloud for Apache Flink and get results.
        Iterate 3 times to get the result or while it is validated.
        Use refinement agent to fix the errors.
        """
        sql_validated = False
        iteration_count = 0
        agent_history = [{"agent": "refinement", "sql": translated_sql}]
        while not sql_validated and iteration_count < 3:
            iteration_count += 1
            sql_validated, status_detail = self._validate_flink_sql_on_cc(translated_sql)
            if not sql_validated:
                print(f"\n\n--> Error: {status_detail}", flush=True)
                print(f"Process with refinement agent {iteration_count}", flush=True)
                if "does not exist, may be on a private cluster, or you do not have permission to access it" in status_detail:
                    print("Missing source tables, so no need to refine as this error has to be fixed by the user", flush=True)
                    return translated_sql, sql_validated
                translated_sql = self._run_refinement_agent(translated_sql, str(agent_history), status_detail)
                print(f"Refined sql:\n {translated_sql}", flush=True)
                agent_history.append({"agent": "refinement", "sql": translated_sql})
                print("do you want to continue? (y/n) or you continue with the generated sql?", flush=True)
                answer = input()
                if answer != "y":
                    return translated_sql, sql_validated
            else:
                print(f"SQL is valid and runs on CC after {iteration_count} iterations", flush=True)
                logger.info(f"SQL is valid and runs on CC after {iteration_count} iterations")
        return translated_sql, sql_validated

    def _snapshot_ddl_dml(self, table_name: str, ddl: str, dml: str):
        """
        Snapshot the DDL and DML statements to a file.
        """
        try:
            if ddl and len(ddl) > 0:
                with open(os.path.join(shift_left_dir, f"ddl.{table_name}.sql"), "w") as f:
                    f.write(ddl)
            if dml and len(dml) > 0:
                with open(os.path.join(shift_left_dir, f"dml.{table_name}.sql"), "w") as f:
                    f.write(dml)
        except Exception as e:
            logger.error(f"Error snapshotting DDL/DML: {e}")

    def _process_syntax_validation(self, sql: str) -> str:
        """
        Process semantic validation of SQL statements.

        This method is a placeholder for future semantic validation logic.
        Semantic validation would check for logical correctness, data flow consistency,
        and business rule compliance beyond basic syntax checking.

        Args:
            sql (str): The SQL statement to validate semantically

        Returns:
            str: The SQL statement (currently unchanged, future enhancement point)
        """
        # TODO: Implement semantic validation logic
        # This could include checks for:
        # - Data type compatibility
        # - Join condition validity
        # - Temporal semantics for streaming queries
        # - Resource usage optimization
        return sql

