# Recipes Summary

This section provides a brief overview of the migration tools and their application to specific use cases. Ensure the following environment variables are set: in a `.env` file. For example, in a project where the source repository is cloned to your-src-dbt-folder and the target Flink project is flink-project, use these setting:

```sh
export SRC_FOLDER=../../your-src-dbt-folder/models
export STAGING=../../flink-project/staging
export PIPELINES=../../flink-project/pipelines
export CONFIG_FILE=../../flink-project/config.yaml
```

Refer to the high-level component view for project organization details:

![](./images/components.drawio.png)

## Project related tasks

### Create a Flink project structure

* Get help for the shift_left project management CLI

```sh
shift_left project --help
```


* To create a new project:

```sh
shift_left project init <project_name> <project_path> --project-type 
# example for a default Kimball project
shift_left project init flink-project ../
# For a project more focused on developing data product
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



## Build an inventory of Flink SQL DDL and DML statements

The inventory is built by crowling the `pipelines` folder and looking at each dml to get the table name. The inventory is a hashmap with the key being the table name and the value is a TableReference defined as:

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


## Work with pipelines

The table inventory, as created in previous recipe, is important to get the pipeline metadata created. The approach is to define metadata for each table in term of DDL and DML + parents and children.

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


### Get parent pipeline for human

???+ info "For structure ouput"
    See the pipeline definition creation in [this section](#build-structured-pipeline-metadata-and-walk-through).

To get the parent hierarchy for a fact or dimension table, use the `pipeline_helper.py` tool. The output will report the dependent tables up to the sources from the inventory of sql files.
This inventory will be built from the source folder specified with the -i or --inventory argument

* Example to get the parents used in a given dbt or SQL source project:

```sh
python pipeline_helper.py -f $SRC_FOLDER/facts/fct_orders.sql -i $SRC_FOLDER
```

* Example of searching the pipeline of a migrated file from the migrated content:

```sh
python pipeline_helper.py -f $PIPELINES/facts/fct_users.sql -i $PIPELINES
```

* The same but for the staging content

```sh
python pipeline_helper.py -f $PIPELINES/facts/fct_users.sql -i $STAGING
```

[See this section for details](./migration.md)

### Get what are the Flink SQL statement  using a specified table

This is helpful to understand what are the "consumers" of a table.

```sh
python find_table_user.py -t table_name -r root_folder_to_search_in
# example
python find_table_user.py -t users -r $PIPELINES
```

### Build structured pipeline metadata and walk through

A pipeline is discovered by walking from the sink to the sources via intermediate statements. Each pipeline is a list of existing dimension and fact tables, and for each table the tool creates the `pipeline_definition.json`.

The structured folder for a table looks like:

`<facts | intermediate | dimensions | sources>/<product_name>/<table_name>`

The `pipeline_definition.json` is persisted under the <table_name> folder.

The tool needs to get an up-to-date inventory, see [previous section to build it](#build-an-inventory-of-flink-sql-dml-statements).

* Build all the `pipeline_definition.json` from a sink:

```sh
python pipeline_worker.py -f $PIPELINES/facts/p1/fct_order/sql-scripts/dml.fct_order.sql  --build
```

### Delete pipeline_defition.json file for a given folder

Delete all the `pipeline_definition.json` files from a given folder. It goes down recursively.

```sh
python pipeline_worker.py -d $PIPELINES/sources/product_1
```

```sh
shift_left pipeline delete-metadata ../../examples/flink-project/pipelines
```

### Build pipeline reports 

* Get a report from one sink to n sources:

```sh
python pipeline_worker.py -f $PIPELINES/facts/product_1/fct_table/pipeline_definition.json --report
```

* Get a report from one source to n sinks:

```sh
python pipeline_worker.py -f $PIPELINES/sources/product_1/src_table/pipeline_definition.json --report
```



## Migration

### How to migrate an existing dbt or sql script to the matching Apache Flink SQL statement 

The tool is `process_src_tables.py` but it will do different migration approach, depending of the type of table: sources or facts,dimensions or intermediates

The syntax is:

```sh
python process_src_tables.py -f $SRC_FOLDER/facts/product_name/fct_users.sql -o $STAGING/facts/product_name --walk-up
```

* Generate Flink SQLs from one Fact or Dimension table using a recurcive processing up to the source tables. The SQL created statements are saved into the staging temporary folder



### Process all the source tables

* For all tables in the `sources` folder, create the matching Flink SQL DDL and  DMLs into a temporary folder. Once generated the deduplication logic can be finalized manually to the final pipelines folder. This approach may not be needed if you use the process parent hierarchy option.

```sh
python process_src_tables.py -f $SRC_FOLDER/sources -o $STAGING/sources
```


## Day to day maintenance

### Update the source sql to do some update

The typical update will be the source table name to be used as it may change per environment. So this tool can help do a global change. Another use case is to limit the data to use for developement to a spesific set of record, so a where statement can be done.

```sh
python change_src_sql.py -s <source_folder> -w "where condition as string"
```

### Create one Makefile definition for a given table

During development we face the need to adapt the Makefile quite often depending of project structure and naming convention. The following code helps to do so:


```sh
python create_table_folder_structure.py --build-makefile -o ../examples/facts/p1/fct_order -t fct_order
```

### Updates to all Makefiles in a folder 

Also if we want to change all the Makefiles within a specific folder and its sub-folders:

```sh
python create_table_folder_structure.py --update-makefiles -o ../examples/facts/
```


## Deploy using the pipeline definition

## Others - Need to be updated

* Once the Source DDL is executed successfully, generate test data, with 5 records, and send them to the matching topic. 

```sh
# -o is for the output file to get the json array of json objects to be send to the topic, -t is for table name, and -n is for the number of records to create
python generate_data_for_table.py -o data.json -t sys_user_raw -n 5 
```

* Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t portal_role_raw -s ../pipelines/sources/portal_role
```