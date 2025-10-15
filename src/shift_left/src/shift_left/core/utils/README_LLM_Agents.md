# LLM Translation Agents for Flink SQL

This directory contains a suite of Large Language Model (LLM) agents designed to translate SQL from various dialects to Apache Flink SQL, with validation and iterative refinement capabilities.

## Architecture Overview

The translation system follows a modular, agent-based architecture with three main components:

```
TranslatorToFlinkSqlAgent (Base Class)
├── SparkToFlinkSqlAgent (Spark SQL → Flink SQL)
└── KsqlToFlinkSqlAgent (KSQL → Flink SQL)
```

## Core Components

### 1. TranslatorToFlinkSqlAgent (`translator_to_flink_sql.py`)

**Purpose**: Base abstract class that defines the common interface and shared functionality for all SQL translation agents.

**Key Features**:
- **LLM Configuration**: Supports multiple models (Qwen, Mistral, Cogito) with configurable endpoints
- **Validation Pipeline**: Integrates with Confluent Cloud for Apache Flink for live SQL validation
- **Factory Pattern**: Provides `get_or_build_sql_translator_agent()` for dynamic agent instantiation
- **Error Handling**: Base framework for iterative refinement when validation fails

**Core Methods**:
```python
def translate_to_flink_sqls(table_name, sql, validate=False) -> Tuple[List[str], List[str]]
def _validate_flink_sql_on_cc(sql_to_validate) -> Tuple[bool, str]
def _iterate_on_validation(translated_sql) -> Tuple[str, bool]
```

### 2. SparkToFlinkSqlAgent (`spark_sql_code_agent.py`)

**Purpose**: Specialized agent for translating Spark SQL to Flink SQL with enhanced error categorization and refinement.

**Translation Workflow**:
```
Spark SQL Input
    ↓
1. Translation Agent (Spark → Flink DML)
    ↓
2. DDL Generation Agent (DML → DDL)
    ↓
3. Pre-validation (Syntax Check)
    ↓
4. Confluent Cloud Validation
    ↓
5. Iterative Refinement (if errors)
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

### 3. KsqlToFlinkSqlAgent (`ksql_code_agent.py`)

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

## Agent Capabilities Comparison

| Feature | Spark Agent | KSQL Agent |
|---------|-------------|------------|
| Multi-table Support | ❌ | ✅ |
| DDL Auto-generation | ✅ | ❌ |
| Error Categorization | ✅ | ❌ |
| Input Preprocessing | Basic | Advanced |
| Validation Iterations | 3 max | 3 max |
| File Snapshots | ❌ | ✅ |

## Usage Examples

### Factory Pattern Usage

Declare the class to be used in the config.yml as:
```yaml
app.translator_to_flink_sql_agent: shift_left.core.utils.spark_sql_code_agent.SparkToFlinkSqlAgent
```

```python
from shift_left.core.utils.translator_to_flink_sql import get_or_build_sql_translator_agent

# Get configured agent (Spark or KSQL based on config)
agent = get_or_build_sql_translator_agent()
ddl_list, dml_list = agent.translate_to_flink_sqls(
    table_name="my_table",
    sql=input_sql,
    validate=True  # Enable live validation
)
```

### Direct Agent Usage
```python
# Spark SQL Translation
spark_agent = SparkToFlinkSqlAgent()
ddl, dml = spark_agent.translate_to_flink_sqls("customer_orders", spark_sql, validate=True)

# KSQL Translation  
ksql_agent = KsqlToFlinkSqlAgent()
ddl_list, dml_list = ksql_agent.translate_from_ksql_to_flink_sql("stream_processing", ksql_script, validate=False)
```

## Configuration

The agents support flexible LLM configuration via environment variables:

```bash
# Model Selection
export SL_LLM_MODEL="qwen3:30b"  # or qwen2.5-coder:32b, mistral-small:latest, etc.

# LLM Endpoint
export SL_LLM_BASE_URL="http://localhost:11434/v1"  # Ollama endpoint
export SL_LLM_API_KEY="your_api_key"

# Agent Selection (in config.yaml)
app:
  translator_to_flink_sql_agent: "shift_left.core.utils.spark_sql_code_agent.SparkToFlinkSqlAgent"
```

## Prompt Engineering

Each agent uses specialized system prompts stored in external files:

```
prompts/
├── spark_fsql/
│   ├── translator.txt       # Main Spark→Flink translation
│   ├── ddl_creation.txt     # DDL generation from DML
│   └── refinement.txt       # Error-based refinement
└── ksql_fsql/
    ├── translator.txt       # Main KSQL→Flink translation
    ├── refinement.txt       # Error-based refinement
    ├── mandatory_validation.txt  # Syntax validation
    └── table_detection.txt  # Multi-table detection
```

## Validation Process

Both agents support two validation modes:

1. **Mandatory Validation**: Always performed, checks syntax and best practices
2. **Live Validation**: Optional, validates against live Confluent Cloud for Apache Flink

The validation process includes:
- Syntax checking
- Semantic validation
- Iterative refinement (up to 3 attempts)
- Human-in-the-loop confirmation for live validation

## Error Handling and Refinement

When validation fails, agents use specialized refinement prompts to:
- Analyze the specific error message
- Consider validation history
- Generate corrected SQL that addresses the identified issues
- Provide explanations of changes made

This creates a robust, self-correcting translation pipeline that improves accuracy through iterative feedback.
