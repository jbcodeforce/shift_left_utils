# SQL Translation Code Review

???- info "Version"
    Created Dec - 2024 
    Updated Nov 25 - 2025
	Updated Feb 2026


The current AI based migration implementation supported by this tool enables migration of:

* dbt/Spark SQL to Flink SQL
* ksqlDB to Flink SQL

The approach uses LLM agents local or remote. This document covers the design approach, development environment setup, and how to execute tests to tune the AI based SQL migrations, specially how to extend the AI prompts or workflows to get better results.

The implementation uses the OpenAI SDK, so different LLM models can be used, as soon as they support OpenAI. The `qwen3:30b` or `qwen-coder-30b-a3b-instruct-mlx-4bit` models can be used locally using Osaurus on Mac M3 Pro with 36GB RAM., or Ollama on Linux VM. Other models running remotely and supporting OpenAI APIs may be used too using your own API key. 

## Review the end-user lab

Review the [end user lab for table migration](../tutorial/migration_ai_lab.md) to understand how data engineers may use the tool for migration. It is important to get RBAC to be able to see Flink statements and submit statements via API.

For a given table, the tool creates one folder with the table name, a Makefile to help manage the Flink statements with Confluent CLI, a `sql-scripts` folder for the Flink DDL and DML statements. This is the same as `shift_left table init <table_name> ` command.

As part of the process, developers need to validate the generated DDL and update the PRIMARY key to reflect the expected key. This information is hidden in many files in dbt, and key extraction is not yet automated by the migration tools.

**Attention**, the DML is not executable until all dependent input tables are created.

## SQL Sources for test

* Spark examples to migrate from are under [src/shift_left/tests/data/spark-project](https://github.com/jbcodeforce/shift_left_utils/tree/main/src/shift_left/tests/data/spark-project)
* The ksqldb files are in [src/shift_left/tests/data/ksql-project/sources](https://github.com/jbcodeforce/shift_left_utils/tree/main/src/shift_left/tests/data/ksql-project/sources)
* PySpark samples are in [src/shift_left/tests/data/pyspark-project](https://github.com/jbcodeforce/shift_left_utils/tree/main/src/shift_left/tests/data/pyspark-project)

### Python test code to run migration

The different code is under [tests/ai](https://github.com/jbcodeforce/shift_left_utils/tree/main/src/shift_left/tests/ai) folder.

* Be sure to have define config file and environment variables
	```sh
	export FLINK_PROJECT=$(pwd)/tests/data/flink-project
	export PIPELINES=$FLINK_PROJECT/pipelines
	export STAGING=$FLINK_PROJECT/staging/
	```
* Be sure to have access to a local LLM, Osaurus (for Mac MLX), or Ollama, then specify:
	```sh
	export SL_LLM_BASE_URL=http://localhost:1337/v1
	export SL_LLM_MODEL=qwen3-coder-30b-a3b-instruct-mlx-4bit
	export SL_LLM_API_KEY=not_needed
	```

#### Spark migration

* Set SRC_FOLDER variable to get Spark SQL source files
	```
	export SRC_FOLDER=$FLINK_PROJECT/../spark-project
	```
* Run the first spark SQL migration with:
	```sh
	uv run pytest -vs tests/ai/test_first_spark_migration.py
	```

	Which is the same as the following command plus test assertions
	```sh
    shift_left table migrate raw_active_users $SRC_FOLDER/sources/users/raw_active_users.sql $STAGING --source-type spark --product-name users
	```

* To run all spark sources
	```sh
	uv run pytest -vs tests/ai/test_all_spark_migration.py
	```

### KSQL migration

* Set SRC_FOLDER variable to get spark source files
	```
	export SRC_FOLDER=$FLINK_PROJECT/../ksql-project
	```
* Run the first spark SQL migration with:
	```sh
	uv run pytest -vs tests/ai/test_first_ksql_migration.py
	```

	Which is the same as the following command plus test assertions
	```sh
    shift_left table migrate filtering $SRC_FOLDER/sources/filtering.sql $STAGING --source-type ksql --product-name orders
	```

* Status of the working migration: See **test_ksql_migration.py::TestKsqlMigrations** code.

| Source | Status | Test Case |
| --- | --- | --- |
| splitting_tutorial | 4 DDLs ✅ 4 DMLs ✅ | test_ksql_splitting_tutorial |
| merge_tutorial | 3 DDLs ✅ 2 DMLs ✅ | test_ksql_merge_tutorial | 

## Current Agentic Approach

The current agentic workflow includes:
1. **Assess** if sql has multiple create table or stream in one file
1. **Translate** the given SQL content to Flink SQL
1. **Validate** the syntax and semantics
1. **Generate** DDL derived from DML, if not translated already
1. **Get human validation** to continue or not the automation
1. **Deploy** and test with validation agents [optional]

The system uses validation agents that execute syntactic validation and automatic deployment, with feedback loops injecting error messages back to translator agents when validation fails.

## Architecture Overview

The multi-agent system with human-in-the-loop validation may use Confluent Cloud's Flink REST API to deploy the generated Flink statement. The following diagram represents the different agents working together:

<figure markdown='span'>
![AI Agent Flow](./images/ai_agent_new_flow.drawio.png)
</figure>


Both agents support two validation modes:

1. **Mandatory Validation**: Always performed, checks syntax and best practices
2. **Live Validation**: Optional, validates against live Confluent Cloud for Apache Flink

The validation process includes:

- Syntax checking
- Semantic validation
- Iterative refinement (up to 3 attempts)
- Human-in-the-loop confirmation for live validation

When validation fails, agents use specialized refinement prompts to:

- Analyze the specific error message
- Consider validation history
- Generate corrected SQL that addresses the identified issues
- Provide explanations of changes made

This creates a self-correcting translation pipeline that improves accuracy through iterative feedback.

### Agent Roles

As agent is a combination of LLM reference, prompts, and tool definitions, there will be different implementation of those agents if we do ksqlDB to Flink SQL or from Spark to Flink.

#### KsqlDB to Flink agents

Supporting class of the workflow is [ksqlDB code agent](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/shift_left/ai/ksql_code_agent.py).

Each agent uses specialized system prompts stored in external files:

| Agent | Scope | Prompt File |
| --- | --- | --- |
| **Translator** | Raw KSQL to Flink SQL translation | `ai/prompts/ksql_fsql/translator.txt` |
| **Table Detection** | Identify multiple CREATE statements | `ai/prompts/ksql_fsql/table_detection.txt` |
| **Validation** | Validate Flink SQL constructs | `ai/prompts/ksql_fsql/mandatory_validation.txt` |
| **Refinement** | Fix deployment errors | `ai/prompts/ksql_fsql/refinement.txt` |

#### Code 

The `src/shift_left/shift_left/ai` directory contains a suite of Large Language Model (LLM) agents designed to translate SQL from various dialects to Apache Flink SQL, with validation and iterative refinement capabilities.

#### Spark to Flink agents

Supporting class of the workflow is [Spark sql code agent](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/shift_left/ai/spark_sql_code_agent.py).

Same approach for spark SQL with the prompts being in the `ai/prompts/spark_fsql` folder.
Each agent uses specialized system prompts stored in external files:

| Agent | Scope | Prompt File |
| --- | --- | --- |
| **Translator** | Spark SQL to Flink SQL translation | `ai/prompts/spark_fsql/translator.txt` |
| **Table Detection** | Identify multiple CREATE statements | `ai/prompts/spark_fsql/table_detection.txt` |
| **Validation** | Validate Flink SQL constructs | `ai/prompts/spark_fsql/mandatory_validation.txt` |
| **Refinement** | Fix deployment errors | `ai/prompts/spark_fsql/refinement.txt` |

### Class diagram

The translation system follows a modular, agent-based architecture with three main components:

```
TranslatorToFlinkSqlAgent (Base Class)
├── SparkToFlinkSqlAgent (Spark SQL → Flink SQL)
└── KsqlToFlinkSqlAgent (KSQL → Flink SQL)
```


![](./images/ai_classes.drawio.png)


#### TranslatorToFlinkSqlAgent (`translator_to_flink_sql.py`)

**Purpose**: Base abstract class that defines the common interface and shared functionality for all SQL translation agents.

**Key Features**:

- **LLM Configuration**: Supports multiple models (Qwen, Mistral, Cogito) with configurable endpoints
- **Validation Pipeline**: Integrates with Confluent Cloud for Apache Flink for live SQL validation
- **Factory Pattern**: Provides `get_or_build_sql_translator_agent()` for dynamic agent instantiation
- **Error Handling**: Base framework for iterative refinement when validation fails


#### SparkToFlinkSqlAgent (`spark_sql_code_agent.py`)

**Purpose**: Specialized agent for translating Spark SQL to Flink SQL with enhanced error categorization and refinement.

**Key Features**:

- **Structured Responses**: Uses Pydantic models for consistent LLM output parsing
- **Error Categorization**: Classifies errors into specific types (syntax, function incompatibility, type mismatch, etc.)
- **Multi-step Validation**: Pre-validation + live validation with up to 3 refinement iterations
- **DDL Auto-generation**: Automatically creates table definitions from query logic
- **Validation History**: Tracks all validation attempts for debugging

**Specialized Models**:
```python
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
```

#### KsqlToFlinkSqlAgent (`ksql_code_agent.py`)

**Purpose**: Specialized agent for translating KSQL (Kafka SQL) to Flink SQL with multi-table support and comprehensive preprocessing.

**Key Features**:

- **Multi-table Processing**: Automatically detects and processes multiple CREATE TABLE/STREAM statements
- **Input Preprocessing**: Removes problematic statements and comments that confuse LLMs
- **Batch Translation**: Handles complex KSQL scripts with multiple related statements
- **Mandatory Validation**: Always performs syntax and best practices validation
- **File Snapshots**: Saves intermediate results for debugging and tracking

**Specialized Models**:
```python
class KsqlFlinkSql(BaseModel):
    ksql_input: str
    flink_ddl_output: Optional[str]
    flink_dml_output: Optional[str]

class KsqlTableDetection(BaseModel):
    has_multiple_tables: bool
    table_statements: List[str]
    description: str
```

## Using Cursor.ai

If you have access to Cursor with  `claude-4.5-sonnet` model or recent model, a prompt like:

```sh
using @src/shift_left/shift_left/ai/prompts/ksql_fsql/translator.txt migrate the @src/shift_left/tests/data/ksql-project/sources/w2_processing.ksql  file
```

Will create a markdown file with the original ksql statements and the flink SQL matching statements at a higher speed than running qwen locally.

Also using the new SKILL.md support, the translator.txt prompt may be use inside any AI agent using skill.
