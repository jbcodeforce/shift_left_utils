# Migration using AI


## Migration Workflow

### 1. Project Initialization

Create a new target project to keep your Flink statements and pipelines (e.g., my-flink-app):

```bash
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
