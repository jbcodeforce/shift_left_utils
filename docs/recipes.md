# Recipes Summary

???- info "Version"
    * Created January 2025.
    * Still under construction


This chapter details the standard activities to manage a Flink project with the shift_left tools when doing a ETL to real-time miration project. The recipes addresses new project initiative or migration project from an existing SQL based ETL solution.

[Refer to the high-level component](./index.md#context) view for project organization details:

![](./images/components.drawio.png)


Ensure the following environment variables are set: in a `.env` file. For example, in a project where the source repository is cloned to your-src-dbt-folder and the target Flink project is flink-project, use these setting:

```sh
export FLINK_PROJECT=$HOME/Code/link-project
export STAGING=$FLINK_PROJECT/staging
export PIPELINES=$FLINK_PROJECT/pipelines
export SRC_FOLDER=$HOME/Code/customers/master-control/de-datawarehouse/models
export CCLOUD_CONTEXT=login.....
export CPOOL_ID=lfcp-xxxxxx
export TOPIC_LIST_FILE=$FLINK_PROJECT/src_topic_list.txt 
export CONFIG_FILE=$FLINK_PROJECT/config.yaml
export DB_NAME=kafka_env_name
export CCLOUD_ENV_ID=env-xxxxxx
```

## Project related tasks

### Create a Flink project structure

This activity is done when starting a new Flink project.

* Get help for the shift_left project management CLI

```sh
shift_left project --help
```

* To create a new project:

```sh
shift_left project init <project_name> <project_path> --project-type 
# example for a default Kimball project
shift_left project init flink-project ../
# For a project more focused on developing data as a product
shift_left project init flink-project ../ --project-type data-product
```

???- info "Output"
        ```sh
        my-flink-project
        ├── config.yaml
        ├── docs
        ├── logs
        ├── pipelines
        │   ├── common.mk
        │   └── data_product_1
        │       ├── dimensions
        │       ├── facts
        │       │   └── fct_order
        │       │       ├── Makefile
        │       │       ├── sql-scripts
        │       │       │   ├── ddl.fct_order.sql
        │       │       │   └── dml.fct_order.sql
        │       │       ├── tests
        │       │       └── tracking.md
        │       ├── intermediates
        │       └── sources
        └── staging
        ```

### Add a table structure to the Flink project

* Get help for the shift_left table management CLI

```sh
shift_left table --help
```

* The goal of this tool is to create a folder structure to start migrating SQL manually:

```sh
shift_left table init fct_user $PIPELINES/facts/p1
```

???- info "Output"

        ```
        └── p1
            └── fct_user
                ├── Makefile
                ├── sql-scripts
                │   ├── ddl.fct_user.sql
                │   └── dml.fct_user.sql
                ├── tests
                └── tracking.md
        ```



### Discover the current source pipeline

The source project, is constructed with the Kimball approach. Developer may want to understand the how the source pipelines are constructed. Existing Data Platforms have such capabilities, but we found interesting to get a simple tool that goes within the source SQL content and build a dependencies graph. The current SQL parser is good to parse dbt SQL file.

Starting with a single fact table, the following command will identify the dependency hierarchy and include elements to track the migration project:

```sh
shift_left table search-source-dependencies $SRC_FOLDER/facts/fact_education_document.sql $SRC_FOLDER
```

* The output may look like in the followring report:

```sh
-- Process file: $SRC_FOLDER/facts/fact_education_document.sql
Table: fact_education_document in the SQL ../facts/fact_education_document.sql  depends o
  - int_education_completed  in SQL ../intermediates/int_education_completed.sql
  - int_non_abandond_training  in SQL ../intermediates/int_non_abandond_training.sql
  - int_unassigned_curriculum  in SQL ../intermediates/int_unassigned_curriculum.sql
  - int_courses   in SQL ../intermediates/int_courses.sql
  - int_docs_wo_training_data  in SQL ../intermediates/docs/int_docs_wo_training_data.sql
```


## Migrate SQL tables from source to staging

As presented in the [introduction](./index.md/#shift_left-tooling), the migration involves a Local LLM running with Ollama, so developers need this environment to be able to run the following commands.

* Process one table

```sql
shift_left table migrate $SRC_FOLDER/facts/aqem/aqem.fct_event.sql $STAGING
```

* Process the table and the parents up to the sources. So it will migrate recursively all the tables. This could take time if the dependencies graph is big.

```sql
shift_left table migrate $SRC_FOLDER/facts/aqem/aqem.fct_event.sql $STAGING --recursive
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


For a given table, the tool creates one folder with the table name, a Makefile to help managing the Flink Statements with Confluent cli, a `sql-scripts` folder for the Flink ddl and dml statements. A `tests` folder to add `test_definitions.yaml` (using another tool) to do some basic testing.

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

As part of the process, developers need to validate the generated DDL and update the PRIMARY key to reflect the expected key. This information is hidden in lot of files in the dbt, and the key extraction is not yet automated by the migration tools, yet.

Normally the DML is not executable until all dependent tables are created.

## Build an inventory of all the Flink SQL DDL and DML statements with table name as key

The inventory is built by crowling the Flink project `pipelines` folder and by looking at each dml to get the table name. The inventory is a hashmap with the key being the table name and the value is a `FlinkTableReference` defined as:

```python
class FlinkTableReference(BaseModel):
    table_name: Final[str] 
    ddl_ref: Optional[str]
    dml_ref: Optional[str]
    table_folder_name: str
```

* To build an inventory file do the following command:

```sh
shift_left table build-inventory $PIPELINES
```

???- info "Example of inventory created"

        ```json
        "src_table_2": {
            "table_name": "src_table_2",
            "dml_ref": "../examples/flink_project/pipelines/sources/p1/src_table_2/sql-scripts/dml.src_table_2.sql",
            "table_folder_name": "../examples/flink_project/pipelines/sources/p1/src_table_2"
        },
        "src_table_3": {
            "table_name": "src_table_3",
            "dml_ref": "../examples/flink_project/pipelines/sources/p1/src_table_3/sql-scripts/dml.src_table_3.sql",
            "table_folder_name": "../examples/flink_project/pipelines/sources/p1/src_table_3"
        },
        "src_table_1": {
            "table_name": "src_table_1",
            "dml_ref": "../examples/flink_project/pipelines/sources/src_table_1/sql-scripts/dml.src_table_1.sql",
            "table_folder_name": "../examples/flink_project/pipelines/sources/src_table_1"
        },
        "int_table_2": {
            "table_name": "int_table_2",
            "dml_ref": "../examples/flink_project/pipelines/intermediates/p1/int_table_2/sql-scripts/dml.int_table_2.sql",
            "table_folder_name": "../examples/flink_project/pipelines/intermediates/p1/int_table_2"
        },
        "int_table_1": {
            "table_name": "int_table_1",
            "dml_ref": "../examples/flink_project/pipelines/intermediates/p1/int_table_1/sql-scripts/dml.int_table_1.sql",
            "table_folder_name": "../examples/flink_project/pipelines/intermediates/p1/int_table_1"
        },
        "fct_order": {
            "table_name": "fct_order",
            "dml_ref": "../examples/flink_project/pipelines/facts/p1/fct_order/sql-scripts/dml.fct_order.sql",
            "table_folder_name": "../examples/flink_project/pipelines/facts/p1/fct_order"
        }
        ```

The `inventory.json` file is saved under the $PIPELINES folder and it is used intensively by the shift_left cli.

???- warning "Update the inventory"
    Each time a new table is added or renamed, it is recommended to run this command. It can even be integrated in a CI pipeline.

## Work with pipelines

The table inventory, as created in previous recipe, is important to get the pipeline metadata created. The approach is to define metadata for each table to keep relevant information for the DDL and DML files the parents and children relationships.

The model look like:

```python
class FlinkStatementHierarchy(BaseModel):
    table_name: str
    type: str
    path: str
    ddl_ref: str
    dml_ref: str
    parents: Optional[Set[FlinkTableReference]]
    children: Optional[Set[FlinkTableReference]]
```

A source table will not have parent, while a sink table will not have children. Intermediate tables have both.

???+ info "For any help of pipeline commands"

    ```sh
    shift_left pipeline --help
    ```

### Build structured pipeline metadata and walk through

A pipeline is discovered by walking from the sink to the sources via intermediate statements. Each pipeline is a list of existing dimension and fact tables, and for each table the tool creates the `pipeline_definition.json` reflecting the data structure presented in previous section.

The structured folder for a table looks like:

`<facts | intermediates | dimensions | sources>/<product_name>/<table_name>`

The `pipeline_definition.json` is persisted under the <table_name> folder.

The tool needs to get an up-to-date inventory, see [previous section to build it](#build-an-inventory-of-flink-sql-ddl-and-dml-statements).

* Build all the `pipeline_definition.json` from a given sink:

```sh
shift_left pipeline build-metadata $PIPELINES/facts/p1/fct_order/sql-scripts/dml.fct_order.sql
# you can add a folder to get the path to the inventory
shift_left pipeline build-metadata $PIPELINES/facts/p1/fct_order/sql-scripts/dml.fct_order.sql $PIPELINES
```

The tool will keep existing file and merge content, as the use case should address when a developer is adding a child table and reuse an existing table.

### Delete pipeline_defition.json files for a given folder

Delete all the `pipeline_definition.json` files from a given folder. The command walk down the folder tree to find table folder.

```sh
shift_left pipeline delete-metadata $PIPELINES
```

Only the facts tables:

```sh
shift_left pipeline delete-metadata $PIPELINES/facts
```

### Build pipeline reports 

* Get a report from one sink table to n sources: 

```sh
shift_left pipeline report fct_table
# explicitly specifying the pipeline folder.
shift_left pipeline report fct_table $PIPELINES
```

* Get a report from one source to n sinks:

```sh
shift_left pipeline report src_table
```

The same approach works for intermediate tables.

### Pipeline Deployment

There are multiple choices to deploy a Flink Statement:

1. During development phase where `confluent cli` is used with an higher level of abstractions delivered by a Makefile within the table folder.
1. Use the CLI to do a controlled deployment of a table. The tool uses the pipeline metadata to walk down the pipeline to change each table with a new version.
1. The CLI has a mode to delete all tables of a pipelines and redeploy each them with a control manner to avoid overloading the Flink JobManager


The deployment will take the full pipeline from the source to sink giving a sink table name.

#### Using Confluent CLI / makefile

Each confluent cli commands are defined in a common makefile, and each sql to be deploy has also a makefile with a set of targets:

* To create the Flink dynamic table using the target Flink compute pool do:

  ```sh
  make create_flink_ddl
  ```

This action creates the Kafka topic with the name of the table and create the schema definitions for the key and the value in the Schema Registry of the Confluent Cloud environment. A DDL execution will terminate and the Flink job statement is set to be Completed.

* Verify the completion of the job using cli:

  ```sh
  make describe_flink_ddl
  ```

* If the table is also controlled by a DML, for example for joins, or deduplication, a DML may be deployed using:

  ```sh
  make create_flink_dml
  ```

* Verify the running job using cli:

  ```sh
  make describe_flink_dml
  ```

* Sometime, developer may need to delete the created topics and schemas, for that the makefile target is:

  ```sh
  make drop_table_<table_name>
  ```

* Each Flink Statement is named, so it may be relevant to delete a created statement with the command:

  ```sh
  make delete_flink_statements
  ```

#### Using the deploy pipeline command

The pipeline management, requirements and approach are details in the [pipeline management chapter.](./pipeline_mgr.md) The following are commands to use for common use cases:

* Deploy a sink table (Fact, Dimension or View)

## Troubleshooting the CLI

When starting the CLI, it creates a logs folder under the $HOME/.shift_left/logs folder. The level of logging is specified in the `config.yaml` file in the app section:

```yaml
app:
  logging: INFO
```


