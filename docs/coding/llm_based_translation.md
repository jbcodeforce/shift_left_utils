# SQL Translation Methodology

???- info "Version"
    Created Dec - 2024 
    Updated Nov 25 - 2025


The current AI based migration implementation supported by this tool enables migration of:

* dbt/Spark SQL to Flink SQL
* ksqlDB to Flink SQL

The approach uses LLM agents local or remote. This document covers the design approach, development environment setup, and how to use the `shift_left` tool for automated SQL migrations. Finally it addresses how to extend the AI prompts or workflows to get better results.

The core idea is to leverage LLMs to understand the source SQL semantics and to translate them to Flink SQLs. 

**This is not production ready, the LLM can generate hallucinations, and one to one mapping between source like ksqlDB or Spark to Flink is sometime not the best approach.** We expect that this agentic solution could be a strong foundation for better results, and can be enhanced over time.

The implementation use the OpenAI SDK, so different LLM models can be used, as soon as they support OpenAI. The `qwen3:30b` or `qwen-coder-30b-a3b-instruct-mlx-4bit` models can be used locally using Osaurus on Mac M3 Pro with 36GB RAM., or Ollama on Linux VM. Other models running remotely and supporting OpenAI APIs may be used too using your own API key. 

## Migration Context

As described in the [introduction](../index.md), at a high level, data engineers need to take a source project, define a new Flink project, perform migrations, run Flink statement deployments, manage pipelines, and write and execute tests:

<figure markdown='span'>
![](../images/components.drawio.png)
<capture>Shift Left project system context</capture>
</figure>

All those tasks are described under the [recipe chapter](../recipes.md) and supported by this CLI.

For automatic migration, LLMs alone might not be sufficient to address complex translations in an automated process. Agents help by specializing in specific steps with feedback loops and retries.

???- info "Complexity of language translation"
      For any programming language translation, we need to start with a corpus of source code. This can be done programmatically from the source language, then for each generated code, implement the semantically equivalent Flink SQL counterparts.

      The goal of corpus creation is to identify common ksqlDB or Spark SQL constructs (joins, aggregations, window functions, UDFs, etc.), then manually translate a smaller, diverse set of queries to establish translation rules. Using these rules, we can generate permutations and variations of queries. It is crucial to test the generated Flink SQL against a test dataset to ensure semantic equivalence.

      Build query pairs to represent the source-to-target set as a corpus. For each query pair, include the relevant table schemas. This is vital for the LLM to understand data types, column names, and relationships. It is not recommended to have different prompts for different parts of a SQL statement, as the LLM's strength comes from the entire context. However, there will still be problems for SQL scripts that have many lines of code, as a 200+ line script will reach thousands of tokens.

      To improve result accuracy, it is possible to use Supervised Fine-tuning techniques:

      * Fine-tune the chosen LLM on the generated code. The goal is for the LLM to learn the translation patterns and nuances between ksqlDB or Spark SQL and Flink SQL.
      * Prompt Engineering: Experiment with different prompt structures during fine-tuning and inference. A good prompt will guide the LLM effectively. The current implementation leverages this type of prompt: e.g., "Translate the following Spark SQL query to Flink SQL, considering the provided schema. Ensure semantic equivalence and valid Flink syntax."
      * For evaluation assessment, it is recommended to add a step to the agentic workflow to validate the syntax of the generated Flink SQL. Better validation involves assessing semantic equivalence by determining if the Flink SQL query produces the same results as the ksqlDB or Spark SQL query on a given dataset.

      For validation, it may be relevant to have a knowledge base of common translation errors. When the Validation Agent reports an error, the Refinement Agent attempts to correct the Flink SQL. It might feed the error message back to the LLM with instructions to fix it. The knowledge base should be populated with human-curated rules for common translation pitfalls.

      It may be important to explain why a translation was done a certain way to better tune prompts. For complex queries or failures, human review ("human in the loop") and correction mechanisms will be essential, with the system learning from these corrections.

### Limitations

LLMs cannot magically translate custom UDFs. This will likely require manual intervention or a separate mapping mechanism. The system should identify and flag untranslatable UDFs.

Flink excels at stateful stream processing. Spark SQL's batch orientation means that translating stateful Spark operations (if they exist) to their Flink streaming counterparts would be highly complex and would likely require significant human oversight or custom rules.

### Spark SQL to Flink SQL

While Spark SQL is primarily designed for batch processing, it can be migrated to Flink real-time processing with some refactoring and tuning. Spark also supports streaming via micro-batching. Most basic SQL operators (SELECT, FROM, WHERE, JOIN) are similar between Spark and Flink.


* Example command to migrate one Spark SQL script
  ```sh
  # set SRC_FOLDER to one of the spark source folder like tests/data/spark-project
  # set STAGING to the folder target to the migrated content
  shift_left table migrate customer_journey $SRC_FOLDER/sources/src_customer_journey.sql $STAGING --source-type spark
  ```


???- info "Example of Output"
    ```sh
    process SQL file ../src-dbt-project/models/facts/fct_examination_data.sql
    Create folder fct_exam_data in ../flink-project/staging/facts/p1

    --- Start translator AI Agent ---
    --- Done translator Agent: 
    INSERT INTO fct_examination_data
    ...
    --- Start clean_sql AI Agent ---
    --- Done Clean SQL Agent: 
    --- Start ddl_generation AI Agent ---
    --- Done DDL generator Agent:
    CREATE TABLE IF NOT EXISTS fct_examination_data (
        `exam_id` STRING,
        `perf_id` STRING,
    ...
    ```

For a given table, the tool creates one folder with the table name, a Makefile to help manage the Flink statements with Confluent CLI, a `sql-scripts` folder for the Flink DDL and DML statements. 

Example of created folders:

```sh
facts
    └── fct_examination_data
        ├── Makefile
        ├── sql-scripts
        │   ├── ddl.fct_examination_data.sql
        │   └── dml.fct_examination_data.sql
        └── tests
```

As part of the process, developers need to validate the generated DDL and update the PRIMARY key to reflect the expected key. This information is hidden in many files in dbt, and key extraction is not yet automated by the migration tools.

**Attention**, the DML is not executable until all dependent input tables are created.
  
### ksqlDB to Flink SQL

ksqlDB has SQL constructs to do stream processing, but this is not an ANSI SQL engine. It is highly integrated with Kafka and uses specific keywords to define such integration. LLM may have limited access to ksql code during the training, so results may not be optimal. 

The migration and prompts need to support more examples outside of the classical SELECT and CREATE TABLE statements.

* The ksqldb files to test, the migration from, are in [src/shift_left/tests/data/ksql-project/sources](https://github.com/jbcodeforce/shift_left_utils/tree/main/src/shift_left/tests/data/ksql-project/sources)
* Set the environment variables to run shift_left using the [setup instructions](../setup.md)
  ```
  PROJECT_ROOT=shift_left_utils/tree/main/src/shift_left/tests/
  export CONFIG_FILE=$PROJECT_ROOT/config.yaml
  export SRC_FOLDER=$PROJECT_ROOT/data/ksql-project/sources
  export STAGING=$PROJECT_ROOT/data/ksql-project/staging
  export SL_LLM_BASE_URL=http://localhost:11434/v1
  export SL_LLM_MODEL=qwen3-coder:30b
  export SL_LLM_API_KEY=not_needed_key
  ```
* Start ollama: 
  ```sh
  osaurus serve
  # or 
  ollama serve
  ```

* Be sure to have one of the following model (`osaurus list` or `ollama list`): By default `qwen-coder-30b-a3b-instruct-mlx-4bit` or `qwen3-coder:30b` is used.
	```sh
	gpt-oss:20b   13 GB        
	qwen-coder-30b-a3b-instruct-mlx-4bit
	```
	
	if not use one of the following command:
	```sh
	ollama pull qwen3-coder:30b 
	#
	osaurus pull qwen-coder-30b-a3b-instruct-mlx-4bit
	```


* Example command to migrate one of ksqlDB script:
  ```sh
  shift_left table migrate w2_processing $SRC_FOLDER/w2_processing.ksql $STAGING --source-type ksql --product-name tax
  ```

* Status of the working migration: See **test_ksql_migration.py::TestKsqlMigrations** code.

| Source | Status | Test Case |
| --- | --- | --- |
| splitting_tutorial | 4 DDLs ✅ 4 DMLs ✅ | test_ksql_splitting_tutorial |
| merge_tutorial | 3 DDLs ✅ 2 DMLs ✅ | test_ksql_merge_tutorial | 

## Current Agentic Approach

The current agentic workflow includes:

1. **Translate** the given SQL content to Flink SQL
1. **Validate** the syntax and semantics
1. **Generate** DDL derived from DML
1. **Get human validation** to continue or not the automation
1. **Deploy** and test with validation agents [optional]

The system uses validation agents that execute syntactic validation and automatic deployment, with feedback loops injecting error messages back to translator agents when validation fails.

## Architecture Overview

The multi-agent system with human-in-the-loop validation may use Confluent Cloud's Flink REST API to deploy a generated Flink statement. The following diagram represents the different agents working together:

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

Supporting class of the workflow is [ksqlDB code agent](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/src/shift_left/ai/ksql_code_agent.py).

Each agent uses specialized system prompts stored in external files:

| Agent | Scope | Prompt File |
| --- | --- | --- |
| **Translator** | Raw KSQL to Flink SQL translation | `ai/prompts/ksql_fsql/translator.txt` |
| **Table Detection** | Identify multiple CREATE statements | `ai/prompts/ksql_fsql/table_detection.txt` |
| **Validation** | Validate Flink SQL constructs | `ai/prompts/ksql_fsql/mandatory_validation.txt` |
| **Refinement** | Fix deployment errors | `ai/prompts/ksql_fsql/refinement.txt` |

#### Code 

The `src/shift_left/ai` directory contains a suite of Large Language Model (LLM) agents designed to translate SQL from various dialects to Apache Flink SQL, with validation and iterative refinement capabilities.

#### Spark to Flink agents

Supporting class of the workflow is [Spark sql code agent](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/src/shift_left/ai/spark_sql_code_agent.py).

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

**Translation Workflow**:

```
Spark SQL Input
    ↓
Translation Agent (Spark → Flink DML)
    ↓
DDL Generation Agent (DML → DDL)
    ↓
Pre-validation (Syntax Check)
    ↓
Confluent Cloud Validation
    ↓
Iterative Refinement (if errors)
    ↓
Final Flink SQL (DDL + DML)
```

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

**Translation Workflow**:
```
KSQL Input
    ↓
1. Input Cleaning (Remove DROP statements, comments)
    ↓
2. Table Detection (Identify multiple CREATE statements)
    ↓
3. Individual Translation (Process each statement separately)
    ↓
4. Mandatory Validation (Syntax + best practices)
    ↓
5. Optional Live Validation (Confluent Cloud)
    ↓
Final Flink SQL Collections (DDL[] + DML[])
```

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


## A test bed

The current project includes in the `tests/data/` folder some examples of Spark and ksql scripts.

```sh
tests/data/ksql-project
├── common.mk
├── flink-references
├── sources
│   ├── aggregation.ksql
│   ├── ddl-basic-table.ksql
│   ├── ddl-bigger-file.ksql
│   ├── ddl-filtering.ksql
│   ├── ddl-g.ksql
│   ├── ddl-geo.ksql
│   ├── ddl-kpi-config-table.ksql
│   ├── ddl-map_substr.ksql
│   ├── ddl-measure_alert.ksql
│   ├── dml-aggregate.ksql
│   ├── filtering.ksql
│   ├── geospacial.ksql
│   ├── merge_tutorial.ksql
│   ├── movements.ksql
│   ├── splitter.ksql
│   ├── splitting_tutorial.ksql
│   └── w2_processing.ksql
```

`flink-references` includes some migrated solutions used as reference for validating migrations.

## Prerequisites and Setup

[See the environment setup for developers section.](../contributing.md/#environment-set-up-for-developers)

