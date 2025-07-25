# CLI

**Usage**:

```console
$ [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

**Commands**:

* `version`: Display the current version of shift-left...
* `project`
* `table`
* `pipeline`

## `version`

Display the current version of shift-left CLI.

**Usage**:

```console
$ version [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `project`

**Usage**:

```console
$ project [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `init`: Create a project structure with a...
* `list-topics`: Get the list of topics for the Kafka...
* `list-compute-pools`: Get the complete list and detail of the...
* `clean-completed-failed-statements`: Delete all statements that are failed and...

### `project init`

Create a project structure with a specified name, target path, and optional project type. 
The project type can be one of `kimball` or `data_product`. 
Kimball will use a structure like 
pipelines/sources
pipelines/facts
pipelines/dimensions
...

**Usage**:

```console
$ project init [OPTIONS] [PROJECT_NAME] [PROJECT_PATH]
```

**Arguments**:

* `[PROJECT_NAME]`: Name of project to create  [default: default_data_project]
* `[PROJECT_PATH]`: [default: ./tmp]

**Options**:

* `--project-type TEXT`: [default: kimball]
* `--help`: Show this message and exit.

### `project list-topics`

Get the list of topics for the Kafka Cluster define in `config.yaml` and save the list in the `topic_list.txt` file under the given folder. Be sure to have a `conflig.yaml` file setup.

**Usage**:

```console
$ project list-topics [OPTIONS] PROJECT_PATH
```

**Arguments**:

* `PROJECT_PATH`: Project path to save the topic list text file.  [required]

**Options**:

* `--help`: Show this message and exit.

### `project list-compute-pools`

Get the complete list and detail of the compute pools of the given environment_id. If the environment_id is not specified, it will use the conflig.yaml
with the [&#x27;confluent_cloud&#x27;][&#x27;environment_id&#x27;]

**Usage**:

```console
$ project list-compute-pools [OPTIONS]
```

**Options**:

* `--environment-id TEXT`: Environment_id to return all compute pool
* `--region TEXT`: Region_id to return all compute pool
* `--help`: Show this message and exit.

### `project clean-completed-failed-statements`

Delete all statements that are failed and completed

**Usage**:

```console
$ project clean-completed-failed-statements [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `table`

**Usage**:

```console
$ table [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `init`: Build a new table structure under the...
* `build-inventory`: Build the table inventory from the...
* `search-source-dependencies`: Search the parent for a given table from...
* `migrate`: Migrate a source SQL Table defined in a...
* `update-makefile`: Update existing Makefile for a given table...
* `update-all-makefiles`: Update all the Makefiles for all the...
* `validate-table-names`: Go over the pipeline folder to assess if...
* `update-tables`: Update the tables with SQL code changes...
* `init-unit-tests`: Initialize the unit test folder and...
* `run-test-suite`: Run all the unit tests or a specified test...
* `delete-tests`: Delete the Flink statements and kafka...
* `explain`: Get the Flink execution plan explanations...

### `table init`

Build a new table structure under the specified path. For example to add a source table structure use for example the command:
`shift_left table init src_table_1 $PIPELINES/sources/p1`

**Usage**:

```console
$ table init [OPTIONS] TABLE_NAME TABLE_PATH
```

**Arguments**:

* `TABLE_NAME`: Table name to build  [required]
* `TABLE_PATH`: Folder Path in which the table folder structure will be created.  [required]

**Options**:

* `--product-name TEXT`: Product name to use for the table. If not provided, it will use the table_path last folder as product name
* `--help`: Show this message and exit.

### `table build-inventory`

Build the table inventory from the PIPELINES path.

**Usage**:

```console
$ table build-inventory [OPTIONS] PIPELINE_PATH
```

**Arguments**:

* `PIPELINE_PATH`: Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `table search-source-dependencies`

Search the parent for a given table from the source project (dbt, sql or ksql folders).
Example: shift_left table search-source-dependencies $SRC_FOLDER/

**Usage**:

```console
$ table search-source-dependencies [OPTIONS] TABLE_SQL_FILE_NAME SRC_PROJECT_FOLDER
```

**Arguments**:

* `TABLE_SQL_FILE_NAME`: Full path to the file name of the dbt sql file  [required]
* `SRC_PROJECT_FOLDER`: Folder name for all the dbt sources (e.g. models)  [env var: SRC_FOLDER; required]

**Options**:

* `--help`: Show this message and exit.

### `table migrate`

Migrate a source SQL Table defined in a sql file with AI Agent to a Staging area to complete the work. 
The command uses the SRC_FOLDER to access to src_path folder.

**Usage**:

```console
$ table migrate [OPTIONS] TABLE_NAME SQL_SRC_FILE_NAME TARGET_PATH
```

**Arguments**:

* `TABLE_NAME`: the name of the table once migrated.  [required]
* `SQL_SRC_FILE_NAME`: the source file name for the sql script to migrate.  [required]
* `TARGET_PATH`: the target path where to store the migrated content (default is $STAGING)  [env var: STAGING; required]

**Options**:

* `--source-type TEXT`: the type of the SQL source file to migrate. It can be ksql, dbt, spark, etc.  [default: spark]
* `--recursive`: Indicates whether to process recursively up to the sources. (default is False)
* `--help`: Show this message and exit.

### `table update-makefile`

Update existing Makefile for a given table or build a new one

**Usage**:

```console
$ table update-makefile [OPTIONS] TABLE_NAME PIPELINE_FOLDER_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to process and update the Makefile from.  [required]
* `PIPELINE_FOLDER_NAME`: Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `table update-all-makefiles`

Update all the Makefiles for all the tables in the given folder. Example: shift_left table update-all-makefiles $PIPELINES/dimensions/product_1

**Usage**:

```console
$ table update-all-makefiles [OPTIONS] FOLDER_NAME
```

**Arguments**:

* `FOLDER_NAME`: Folder from where all the Makefile will be updated. If not provided, it will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `table validate-table-names`

Go over the pipeline folder to assess if table name,  naming convention, and other development best practices are respected.

**Usage**:

```console
$ table validate-table-names [OPTIONS] PIPELINE_FOLDER_NAME
```

**Arguments**:

* `PIPELINE_FOLDER_NAME`: Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `table update-tables`

Update the tables with SQL code changes defined in external python callback. It will read dml or ddl and apply the updates.

**Usage**:

```console
$ table update-tables [OPTIONS] FOLDER_TO_WORK_FROM
```

**Arguments**:

* `FOLDER_TO_WORK_FROM`: Folder from where to do the table update. It could be the all pipelines or subfolders.  [required]

**Options**:

* `--ddl`: Focus on DDL processing. Default is only DML
* `--both-ddl-dml`: Run both DDL and DML sql files
* `--string-to-change-from TEXT`: String to change in the SQL content
* `--string-to-change-to TEXT`: String to change in the SQL content
* `--class-to-use TEXT`: [default: typing.Annotated[str, &lt;typer.models.ArgumentInfo object at 0x108aa8b90&gt;]]
* `--help`: Show this message and exit.

### `table init-unit-tests`

Initialize the unit test folder and template files for a given table. It will parse the SQL statemnts to create the insert statements for the unit tests.
It is using the table inventory to find the table folder for the given table name.

**Usage**:

```console
$ table init-unit-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--help`: Show this message and exit.

### `table run-test-suite`

Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results

**Usage**:

```console
$ table run-test-suite [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--test-case-name TEXT`: Name of the individual unit test to run. By default it will run all the tests
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--help`: Show this message and exit.

### `table delete-tests`

Delete the Flink statements and kafka topics used for unit tests for a given table.

**Usage**:

```console
$ table delete-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--help`: Show this message and exit.

### `table explain`

Get the Flink execution plan explanations for a given table or a group of table using the product name.

**Usage**:

```console
$ table explain [OPTIONS]
```

**Options**:

* `--table-name TEXT`: Name of the table to get Flink execution plan explanations from.
* `--product-name TEXT`: The directory to run the explain on each tables found within this directory. table or dir needs to be provided.
* `--table-list-file-name TEXT`: The file containing the list of tables to deploy.
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--persist-report`: Persist the report in the shift_left_dir folder.
* `--help`: Show this message and exit.

## `pipeline`

**Usage**:

```console
$ pipeline [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `build-metadata`: Build a pipeline definition metadata by...
* `delete-all-metadata`: Delete all pipeline definition json files...
* `build-all-metadata`: Go to the hierarchy of folders for...
* `report`: Generate a report showing the static...
* `deploy`: Deploy a pipeline from a given table name...
* `build-execution-plan`: From a given table, this command goes all...
* `report-running-statements`: Assess for a given table, what are the...
* `undeploy`: From a given sink table, this command goes...
* `prepare`: Execute the content of the sql file, line...

### `pipeline build-metadata`

Build a pipeline definition metadata by reading the Flink dml SQL content for the given dml file.

**Usage**:

```console
$ pipeline build-metadata [OPTIONS] DML_FILE_NAME PIPELINE_PATH
```

**Arguments**:

* `DML_FILE_NAME`: The path to the DML file. e.g. $PIPELINES/table-name/sql-scripts/dml.table-name.sql  [required]
* `PIPELINE_PATH`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline delete-all-metadata`

Delete all pipeline definition json files from a given folder path

**Usage**:

```console
$ pipeline delete-all-metadata [OPTIONS] PATH_FROM_WHERE_TO_DELETE
```

**Arguments**:

* `PATH_FROM_WHERE_TO_DELETE`: Delete metadata pipeline_definitions.json in the given folder  [required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline build-all-metadata`

Go to the hierarchy of folders for dimensions, views and facts and build the pipeline definitions for each table found using recursing walk through

**Usage**:

```console
$ pipeline build-all-metadata [OPTIONS] PIPELINE_PATH
```

**Arguments**:

* `PIPELINE_PATH`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline report`

Generate a report showing the static pipeline hierarchy for a given table using its pipeline_definition.json

**Usage**:

```console
$ pipeline report [OPTIONS] TABLE_NAME INVENTORY_PATH
```

**Arguments**:

* `TABLE_NAME`: The table name containing pipeline_definition.json. e.g. src_aqem_tag_tag. The name has to exist in inventory as a key.  [required]
* `INVENTORY_PATH`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--yaml`: Output the report in YAML format
* `--json`: Output the report in JSON format
* `--graph`: Output the report in Graphical tree
* `--children-too / --no-children-too`: By default the report includes only parents, this flag focuses on getting children  [default: no-children-too]
* `--parent-only / --no-parent-only`: By default the report includes only parents  [default: parent-only]
* `--output-file-name TEXT`: Output file name to save the report.
* `--help`: Show this message and exit.

### `pipeline deploy`

Deploy a pipeline from a given table name , product name or a directory.

**Usage**:

```console
$ pipeline deploy [OPTIONS] INVENTORY_PATH
```

**Arguments**:

* `INVENTORY_PATH`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--table-name TEXT`: The table name containing pipeline_definition.json.
* `--product-name TEXT`: The product name to deploy.
* `--table-list-file-name TEXT`: The file containing the list of tables to deploy.
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will create a pool.
* `--dml-only / --no-dml-only`: By default the deployment will do DDL and DML, with this flag it will deploy only DML  [default: no-dml-only]
* `--may-start-descendants / --no-may-start-descendants`: The children deletion will be done only if they are stateful. This Flag force to drop table and recreate all (ddl, dml)  [default: no-may-start-descendants]
* `--force-ancestors / --no-force-ancestors`: When reaching table with no ancestor, this flag forces restarting running Flink statements.  [default: no-force-ancestors]
* `--cross-product-deployment / --no-cross-product-deployment`: By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products.  [default: no-cross-product-deployment]
* `--dir TEXT`: The directory to deploy the pipeline from. If not provided, it will deploy the pipeline from the table name.
* `--parallel / --no-parallel`: By default the deployment will deploy the pipeline in parallel. This flag will deploy the pipeline in parallel.  [default: no-parallel]
* `--help`: Show this message and exit.

### `pipeline build-execution-plan`

From a given table, this command goes all the way to the full pipeline and assess the execution plan taking into account parent, children
and existing Flink Statement running status.

**Usage**:

```console
$ pipeline build-execution-plan [OPTIONS] INVENTORY_PATH
```

**Arguments**:

* `INVENTORY_PATH`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--table-name TEXT`: The table name to deploy from. Can deploy ancestors and descendants.
* `--product-name TEXT`: The product name to deploy from. Can deploy ancestors and descendants of the tables part of the product.
* `--dir TEXT`: The directory to deploy the pipeline from.
* `--table-list-file-name TEXT`: The file containing the list of tables to deploy.
* `--compute-pool-id TEXT`: Flink compute pool ID to use as default.
* `--dml-only / --no-dml-only`: By default the deployment will do DDL and DML, with this flag it will deploy only DML  [default: no-dml-only]
* `--may-start-descendants / --no-may-start-descendants`: The descendants will not be started by default. They may be started differently according to the fact they are stateful or stateless.  [default: no-may-start-descendants]
* `--force-ancestors / --no-force-ancestors`: This flag forces restarting running ancestorsFlink statements.  [default: no-force-ancestors]
* `--cross-product-deployment / --no-cross-product-deployment`: By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products.  [default: no-cross-product-deployment]
* `--help`: Show this message and exit.

### `pipeline report-running-statements`

Assess for a given table, what are the running dmls from its descendants. When the directory is specified, it will report the running statements from all the tables in the directory.

**Usage**:

```console
$ pipeline report-running-statements [OPTIONS] [INVENTORY_PATH]
```

**Arguments**:

* `[INVENTORY_PATH]`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; default: tests/data/ksql-project/flink-references/]

**Options**:

* `--dir TEXT`: The directory to report the running statements from. If not provided, it will report the running statements from the table name.
* `--table-name TEXT`: The table name containing pipeline_definition.json to get child list
* `--product-name TEXT`: The product name to report the running statements from.
* `--from-date TEXT`: The date from which to report the metrics from. Format: YYYY-MM-DDThh:mm:ss
* `--help`: Show this message and exit.

### `pipeline undeploy`

From a given sink table, this command goes all the way to the full pipeline and delete tables and Flink statements not shared with other statements.

**Usage**:

```console
$ pipeline undeploy [OPTIONS] [INVENTORY_PATH]
```

**Arguments**:

* `[INVENTORY_PATH]`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; default: tests/data/ksql-project/flink-references/]

**Options**:

* `--table-name TEXT`: The sink table name from where the undeploy will run.
* `--product-name TEXT`: The product name to undeploy from
* `--help`: Show this message and exit.

### `pipeline prepare`

Execute the content of the sql file, line by line as separate Flink statement. It is used to alter table. 
For deployment by adding the necessary comments and metadata.

**Usage**:

```console
$ pipeline prepare [OPTIONS] SQL_FILE_NAME
```

**Arguments**:

* `SQL_FILE_NAME`: The sql file to prepare tables from.  [required]

**Options**:

* `--compute-pool-id TEXT`: Flink compute pool ID to use as default.
* `--help`: Show this message and exit.
