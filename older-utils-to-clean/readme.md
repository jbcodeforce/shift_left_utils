
## Summary of commands

This section is quick summary of the full content of this readme.

* Get the parent hierarchy for a fact or dimension table:

```shs
python find_pipeline.py -f $SRC_FOLDER/facts/fact_orders.sql
```

* Generate from dbt source tables the matching Flink SQL DDL and Dedup DMLs into a temporary folder to finalize manually to the final pipelines folder:

```sh
python process_src_tables.py -f $SRC_FOLDER/sources -o $STAGING/sources
```

* Generate Flink SQL from one dbt file to the staging temporary folder

```sh
python process_src_tables.py -f $SRC_FOLDER/facts/fact_orders.sql -o $STAGING/orders
```

* Once the Source DDL is executed successful, generate test data, 5 records, to send to the matching topic. It is 

```sh
# -o is for the output file to get the json array of json objects to be send to the topic, -t is for table name, and -n is for the number of records to create
python generate_data_for_table.py -o data.json -t fact_orders -n 5 
```

* Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t fact_orders -s $STAGING/pipelines/sources/orders
```

## Source and target folder structure

The dbt project contains all the SQL statements for migration, located in the models folder. The objective of the tools is to process these files and replicate the same organizational structure for Flink SQL statements, which includes sources, intermediates, staging, dimensions, and facts. Additionally, the tools aim to automate parts of the migration process


The target structure will look like in the following example:

```sh
pipelines
├── dimensions
│       ├── dim_users
│       │   ├── Makefile
│       │   ├── sql-scripts
│       │   │   ├── ddl.dim_users.sql
│       │   │   └── dml.dim_users.sql
│       │   └── tests
│       └── dim_orders
│           ├── Makefile
│           ├── sql-scripts
│           │   ├── ddl.dim_orders.sql
│           │   └── dml.dim_orders.sql
│           └── tests
├── facts
│       ├── fact_orders
│       │   ├── Makefile
│       │   ├── sql-scripts
│       │   │   ├── ddl.fact_orders.sql
│       │   │   └── dml.fact_orders.sql
│       │   └── tests
├── intermediates
│   ├── int_products
│   │   ├── Makefile
│   │   ├── sql-scripts
│   │   │   ├── ddl.int_products.sql
│   │   │   └── dml.int_products.sql
│   │   └── tests

├── sources
│   ├── orders
│   │   ├── Makefile
│   │   ├── sql-scripts
│   │   │   └── dml.orders.sql
│   │       └── ddl.orders.sql
│   │   └── tests
│   ├── users
│   │   ├── Makefile
│   │   ├── sql-scripts
│   │   │   └── dml.users.sql
│   │       └── ddl.users.sql
│   │   └── tests


```

## 1- From a dbt file get all the parent tables

The tool is `find_pipeline.py` which needs only one argument, the sql file name of the fact or dimension table:

```sh
# under utils folder
python find_pipeline.py -f $SRC_FOLDER/facts/fact_users.sql
```

Source tables have no parent. 

## Create one Flink sql from one dbt sql file

This tool is based on local Ollama server with the `qwen2.5-coder:32b` LLM and some agents working with different prompts. This tool does not need to be used for migration, it is more for prompt tuning. There is a more complete process in the following section that uses function of this python module withing other tools that help automating the migration. 

```sh
python flink_sql_code_agent_lg.py -n dbt-file.sql
```

## Process a hierarchy from one single sink sql file

The `process_src_tables.py` is really a set of integrated functions to process a lot of source dbt files and help to migrate more efficiently. It uses 
the LLM LangGraph workflow as defined in `flink_sql_code_agent_lg.py`

The command arguments are:

| arg | meaning |
| --- | --- |
| -f  or --folder_path | name of the folder or file name including the tables to migrate. This Could be a unique .sql file only |
| -o  or --pipeline_folder_path | name of the folder output of the pipelines. Mandatory |
| -t or --table_name | name of the table to process - as dependencies are derived from previous, it is useful to give the table name as parameter and search for the file then process the sql file. This is to follow the process table by table |
| -ld |  default= False, For the given file or folder, list for each table their table dependancies |
| -pd' | default= False, For the given file, process also its dependencies so a full graph is created NOT SUPPORTED" |


**Attention** `-pd` is for taking a table and process all its parent up to all sources: it can be long and give a lot of file to process. It is not yet fully implemented.

The following example will create a DML based on the same logic as the one in the sql script, and will add corresponding DDL to create the sink table:

```sh
python process_src_tables.py -f $SRC_FOLDER/facts/fact_users.sql -o $STAGING/users
```

The tool also lists the dependencies in term of parent tables, up to the source files. It reuses the same functions of the `find_pipeline.py` module:

```sh
Process the table: fact_users
Dependencies found:
  - depends on : int_users_deduped
  - depends on : int_user_roles_deduped
  - depends on : int_user_groups_deduped
```

For a given table, the tool creates one folder with the table name, a Makefile to help managing the Flink Statements with confluent cli, a `sql-scripts` folder for Flink ddl and dml statements. A `tests` folder to add `data.json` (using another tool) to do some basic testing.

As part of the process, developers need to validate the generated DDL and update the PRIMARY key to refect the expected key. This information is hidden in lot of files in the dbt, and it is not yet fully automated for the migration.

Normally the DML is not executable until all dependent tables are created.

With the Makefile the following commands should work for any table: you need to be logged to confluent cloud using `confluent login` command.

```sh
make create_flink_ddl
```

Sometime we may need to delete the created topics and schemas, for that the makefile target is:

```sh
make delete_data
```

Each Flink Statements are named, so it may be relevant to delete a created statement with:

```sh
make delete_flink_statements
```

The `process_src_tables.py` creates or updates two tracking files under `process_out/` folder:

1. `tables_done.txt`  which lists the files processed so far by the different runs. It also manually updated to track the testing.
1. `tables_to_process.txt`: as new dependencies tables are found, they are added to this file so we can track the things to be done

`table_done.txt` can be manually updated to add project management information like if the DDL (True or False), DML (True or False) were successfuly completed and running on CCloud, the source target file (URI_SRC) and the new Flink SQL matching file (URI_TGT).

```sh
table_name, Type of dbt, DDL, DML, URI_SRC, URI_TGT
int_user_roles,fact,T,T,intermediates/int_user_roles.sql,pipelines/intermediates/user_role
```

## Create test data from schema registry 

Once the source DDL is completed in Flink, the Topic is created in Kafka and the value and key schemas are published to the schema registry. In future development those topics and schemas are created in the MC platform.

```sh
python generate_data_for_table.py -o data.json -t fact_users -n 5 
```

Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t portal_role_raw -s ../pipelines/sources/portal_role
```