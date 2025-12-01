# Lab: Migration using AI

The current AI based migration implementation supported by this tool enables migration of:

* dbt/Spark SQL to Flink SQL
* ksqlDB to Flink SQL

The approach uses LLM agents local or remote. After this lab you should be able to use the `shift_left` tool to partially automate your SQL migration to Flink SQL.

The core idea is to leverage LLMs to understand the source SQL semantics and to translate them to Flink SQLs. 

**This is not production ready, the LLM can generate hallucinations, and one to one mapping between source like ksqlDB or Spark to Flink is sometime not the best approach.** We expect that this agentic solution could be a strong foundation for better results, and can be enhanced over time.

## Prerequisites

Be sure to have done the [Setup Lab](./setup_lab.md) to get shift_left operational.

For the 


## Migration Context

As described in the [introduction](../index.md), at a high level, data engineers need to take a source project, define a new Flink project, perform migrations, run Flink statement deployments, manage pipelines, and write and execute tests:

<figure markdown='span'>
![](../images/components.drawio.png)
<capture>Shift Left project system context</capture>
</figure>

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

While Spark SQL is primarily designed for batch processing, it can be migrated to Flink real-time processing with some refactoring and tuning. Spark also supports streaming via micro-batching. Most basic SQL operators (SELECT, FROM, WHERE, JOIN) are similar between Spark and Flink. Some Spark SQL built-in functions need different mapping to Flink built-in functions or may be some UDFs. 


* Example command to migrate one Spark SQL script
  ```sh
  # set SRC_FOLDER to one of the spark source folder like tests/data/spark-project
  # set STAGING to the folder target to the migrated content
  shift_left table migrate customer_journey $SRC_FOLDER/sources/src_customer_journey.sql $STAGING --source-type spark
  ```

### ksqlDB to Flink SQL

ksqlDB has SQL constructs to do stream processing, but this is not an ANSI SQL engine. It is highly integrated with Kafka and uses specific keywords to define such integration. LLM may have limited access to ksql code during the training, so results may not be optimal. 

The migration and prompts need to support more examples outside of the classical SELECT and CREATE TABLE statements.

* Example command to migrate one of ksqlDB script:
  ```sh
  shift_left table migrate w2_processing $SRC_FOLDER/w2_processing.ksql $STAGING --source-type ksql --product-name tax
  ```

## Migration Workflow

### 1. Project Initialization

Create a new target project to keep your Flink statements and different pipelines (e.g., my-flink-app):

```bash
mkdir $HOME/Code
# Initialize project structure
shift_left project init <your-project> </path/to/your/folder>
# example 
shift_left project init my-flink-app $HOME/Code
```

You should get:
```sh
my-flink-app
├── README.md
├── docs
├── pipelines
│   ├── common.mk
│   ├── dimensions
│   ├── facts
│   ├── intermediates
│   ├── sources
│   └── views
├── sources
└── staging
```

### 2. Specific Setup

* Copy your KSQL files to the sources directory:

```bash
# Copy KSQL files
cp *.ksql ${SRC_FOLDER}/
```

* *Optional*: To run locally on a smaller model, download [Ollama](https://ollama.com/download), then install the qwen2.5-coder model:
  ```sh
  # Select the size of the model according to your memory
  ollama pull qwen2.5-coder:32b
  ollama list
  ```

* Create an OpenRouter.ai API key: [https://openrouter.ai/](https://openrouter.ai/), to get access to larger models, like `qwen/qwen3-coder:free` which is free to use.

* Set environment variables:

### 3. Migration Execution

#### Basic Table Migration

```bash
# Migrate a simple table
shift_left table migrate basic_user_table $SRC_FOLDER/user-table.ksql $STAGING --source-type ksql 
```

The command generates:
```sh
# ├── staging/basic_user_table/sql-scripts
# │   ├── ddl.basic_user_table.sql     # Flink DDL
# │   ├── dml.basic_user_table.sql     # Flink DML (if any)
```

### 4. Validation and Deployment

```bash
# Deploy to Confluent Cloud for Flink
cd ${STAGING}/basic_user_table

# Deploy DDL statements
make create_flink_ddl

# Deploy DML statements  
make create_flink_dml
```

### 5. Prepare for pipeline management

Flink statements have dependencies, so it is important to use shift_left to manage those dependencies:

* Run after new tables are created
  ```sh
  shift_left table build-inventory
  ```

* Build all the metadata
  ```sh
  shift_left pipeline build-all-metadata 
  ```


* Verify an execution plan
  ```sh
  shift_left pipeline build-execution-plan --table-name <>
  ```


### 6. Next

* Organize the Flink statements into pipeline folders, possibly using sources, intermediates, dimensions, and facts classification. Think about data products. A candidate hierarchy may look like this:
  ```sh
  my-flink-app

  ├── pipelines
  │   ├── common.mk
  │   ├── dimensions
  │   │   ├── data_product_a
  │   ├── facts
  │   │   ├── data_product_a
  │   ├── intermediates
  │   │   ├── data_product_a
  │   ├── sources
  │   │   ├── data_product_a
  │   │       ├── src_stream
  │   │       │   ├── Makefile
  │   │       │   ├── pipeline_definition.json
  │   │       │   ├── sql-scripts
  │   │       │   │   ├── ddl.src_stream.sql
  │   │       │   │   └── dml.src_stream.sql
  │   │       │   ├── tests
  │   ├── views
          └── data_product_a
  
  ```

* Add unit tests per table (at least for the complex DML ones) ([see test harness](./test_harness.md))
* Add source data into the first tables of the pipeline
* Verify the created records within the sink tables.
