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
* `delete-all-compute-pools`: Delete all compute pools for the given...
* `housekeep-statements`: Delete statements in FAILED or COMPLETED...
* `validate-config`: Validate the config.yaml file
* `report-table-cross-products`: Report the list of tables that are...
* `list-tables-with-one-child`: Report the list of tables that have...
* `list-modified-files`: Get the list of files modified in the...
* `init-integration-tests`: Initialize integration test structure for...
* `run-integration-tests`: Run integration tests for a given sink table.
* `delete-integration-tests`: Delete all integration test artifacts...
* `isolate-data-product`: Isolate the data product from the project
* `get-statement-list`: Get the list of statements

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

### `project delete-all-compute-pools`

Delete all compute pools for the given product name

**Usage**:

```console
$ project delete-all-compute-pools [OPTIONS] PRODUCT_NAME
```

**Arguments**:

* `PRODUCT_NAME`: The product name to delete all compute pools for  [required]

**Options**:

* `--help`: Show this message and exit.

### `project housekeep-statements`

Delete statements in FAILED or COMPLETED state that starts with string &#x27;workspace&#x27; in it ( default ).
Applies optional starts-with and age filters when provided.

**Usage**:

```console
$ project housekeep-statements [OPTIONS]
```

**Options**:

* `--starts-with TEXT`: Statements names starting with this string.
* `--status TEXT`: Statements with this status.
* `--age INTEGER`: Statements with created_date &gt;= age (days).
* `--help`: Show this message and exit.

### `project validate-config`

Validate the config.yaml file

**Usage**:

```console
$ project validate-config [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

### `project report-table-cross-products`

Report the list of tables that are referenced in other products

**Usage**:

```console
$ project report-table-cross-products [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

### `project list-tables-with-one-child`

Report the list of tables that have exactly one child table

**Usage**:

```console
$ project list-tables-with-one-child [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

### `project list-modified-files`

Get the list of files modified in the current git branch compared to the specified branch.
Filters for Flink-related files (by default SQL files) and saves the list to a text file.
This is useful for identifying which Flink statements need to be redeployed in a blue-green deployment.

**Usage**:

```console
$ project list-modified-files [OPTIONS] BRANCH_NAME
```

**Arguments**:

* `BRANCH_NAME`: Git branch name to compare against (e.g., &#x27;main&#x27;, &#x27;origin/main&#x27;)  [required]

**Options**:

* `--project-path TEXT`: Project path where git repository is located  [default: .]
* `--file-filter TEXT`: File extension filter (e.g., &#x27;.sql&#x27;, &#x27;.py&#x27;)  [default: .sql]
* `--since TEXT`: Date from which the files were modified (e.g., &#x27;YYYY-MM-DD&#x27;)
* `--help`: Show this message and exit.

### `project init-integration-tests`

Initialize integration test structure for a given sink table.
Creates test scaffolding including synthetic data templates and validation queries.
Integration tests validate end-to-end data flow from source tables to the specified sink table.

**Usage**:

```console
$ project init-integration-tests [OPTIONS] SINK_TABLE_NAME
```

**Arguments**:

* `SINK_TABLE_NAME`: Name of the sink table to create integration tests for  [required]

**Options**:

* `--project-path TEXT`: Project path where pipelines are located. If not provided, uses $PIPELINES environment variable  [env var: PIPELINES]
* `--help`: Show this message and exit.

### `project run-integration-tests`

Run integration tests for a given sink table.
Executes end-to-end pipeline testing by injecting synthetic data into source tables,
waiting for data to flow through the pipeline, and validating results in the sink table.
Optionally measures latency from data injection to sink table arrival.

**Usage**:

```console
$ project run-integration-tests [OPTIONS] SINK_TABLE_NAME
```

**Arguments**:

* `SINK_TABLE_NAME`: Name of the sink table to run integration tests for  [required]

**Options**:

* `--scenario-name TEXT`: Specific test scenario to run. If not specified, runs all scenarios
* `--project-path TEXT`: Project path where pipelines are located  [env var: PIPELINES]
* `--compute-pool-id TEXT`: Flink compute pool ID. Uses config.yaml value if not provided  [env var: CPOOL_ID]
* `--measure-latency / --no-measure-latency`: Whether to measure end-to-end latency from source to sink  [default: measure-latency]
* `--output-file TEXT`: File path to save detailed test results
* `--help`: Show this message and exit.

### `project delete-integration-tests`

Delete all integration test artifacts (Flink statements and Kafka topics) for a given sink table.
This cleanup command removes all test-related resources created during integration test execution.

**Usage**:

```console
$ project delete-integration-tests [OPTIONS] SINK_TABLE_NAME
```

**Arguments**:

* `SINK_TABLE_NAME`: Name of the sink table to delete integration test artifacts for  [required]

**Options**:

* `--project-path TEXT`: Project path where pipelines are located  [env var: PIPELINES]
* `--compute-pool-id TEXT`: Flink compute pool ID where test artifacts were created  [env var: CPOOL_ID]
* `--no-confirm`: Skip confirmation prompt and delete immediately
* `--help`: Show this message and exit.

### `project isolate-data-product`

Isolate the data product from the project

**Usage**:

```console
$ project isolate-data-product [OPTIONS] PRODUCT_NAME SOURCE_FOLDER TARGET_FOLDER
```

**Arguments**:

* `PRODUCT_NAME`: Product name to isolate  [required]
* `SOURCE_FOLDER`: Source folder to isolate the data product  [required]
* `TARGET_FOLDER`: Target folder to isolate the data product  [required]

**Options**:

* `--help`: Show this message and exit.

### `project get-statement-list`

Get the list of statements

**Usage**:

```console
$ project get-statement-list [OPTIONS] COMPUTE_POOL_ID
```

**Arguments**:

* `COMPUTE_POOL_ID`: Compute pool id to get the statement list for  [required]

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
* `run-unit-tests`: Run all the unit tests or a specified test...
* `run-validation-tests`: Run only the validation tests (1 to n...
* `validate-unit-tests`: just a synonym for run-validation-tests
* `delete-unit-tests`: Delete the Flink statements and kafka...
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
* `--validate`: Validate the migrated sql using Confluent Cloud for Flink.
* `--product-name TEXT`: Product name to use for the table. If not provided, it will use the table_path last folder as product name
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
* `--class-to-use TEXT`: [default: typing.Annotated[str, &lt;typer.models.ArgumentInfo object at 0x1211c6b10&gt;]]
* `--help`: Show this message and exit.

### `table init-unit-tests`

Initialize the unit test folder and template files for a given table. It will parse the SQL statements to create the insert statements for the unit tests.
It is using the table inventory to find the table folder for the given table name.
Optionally, it can also create a CSV file for the unit test data if --create-csv is set.

**Usage**:

```console
$ table init-unit-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--create-csv`: If set, also create a CSV file for the unit test data.
* `--nb-test-cases INTEGER`: Number of test cases to create. Default is 2.  [default: 2]
* `--ai`: Use AI to generate test data and validate with tool calling.
* `--help`: Show this message and exit.

### `table run-unit-tests`

Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results

**Usage**:

```console
$ table run-unit-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--test-case-name TEXT`: Name of the individual unit test to run. By default it will run all the tests
* `--run-all`: By default run insert sqls and foundations, with this flag it will also run validation sql too.
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--post-fix-unit-test TEXT`: Provide a unique post fix (e.g _foo) to avoid conflicts with other UT runs. If not provided will use config.yaml, if that doesnt exist, use default _ut.
* `--help`: Show this message and exit.

### `table run-validation-tests`

Run only the validation tests (1 to n validation tests) for a given table.

**Usage**:

```console
$ table run-validation-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--test-case-name TEXT`: Name of the individual unit test to run. By default it will run all the tests
* `--run-all`: With this flag, and not test case name provided, it will run all the validation sqls.
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--post-fix-unit-test TEXT`: By default it is _ut. A Unique post fix to avoid conflict between multiple UT runs. If not provided, it will use config.yaml one.
* `--help`: Show this message and exit.

### `table validate-unit-tests`

just a synonym for run-validation-tests

**Usage**:

```console
$ table validate-unit-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--test-case-name TEXT`: Name of the individual unit test to run. By default it will run all the tests
* `--run-all`: With this flag, and not test case name provided, it will run all the validation sqls.
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--post-fix-unit-test TEXT`: By default it is _ut. A Unique post fix to avoid conflict between multiple UT runs. If not provided, it will use config.yaml one.
* `--help`: Show this message and exit.

### `table delete-unit-tests`

Delete the Flink statements and kafka topics used for unit tests for a given table.

**Usage**:

```console
$ table delete-unit-tests [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will use config.yaml one.  [env var: CPOOL_ID]
* `--post-fix-unit-test TEXT`: By default it is _ut. A Unique post fix to avoid conflict between multiple UT runs. If not provided, it will use config.yaml one.
* `--help`: Show this message and exit.

### `table explain`

Get the Flink execution plan explanations for a given table or a group of tables using the product name or a list of tables from a file.

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
* `healthcheck`: Generate a healthcheck report of a given...
* `deploy`: Deploy a pipeline from a given table name...
* `build-execution-plan`: From a given table, this command goes all...
* `report-running-statements`: Assess for a given table, what are the...
* `undeploy`: From a given sink table, this command goes...
* `prepare`: Execute the content of the sql file, line...
* `analyze-pool-usage`: Analyze compute pool usage and assess...

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

* `PATH_FROM_WHERE_TO_DELETE`: Delete metadata pipeline_definitions.json in the given folder  [env var: PIPELINES; required]

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
* `--children-too / --no-children-too`: By default the report includes only parents, this flag focuses on getting children  [default: no-children-too]
* `--parent-only / --no-parent-only`: By default the report includes only parents  [default: parent-only]
* `--output-file-name TEXT`: Output file name to save the report.
* `--help`: Show this message and exit.

### `pipeline healthcheck`

Generate a healthcheck report of a given product pipeline

**Usage**:

```console
$ pipeline healthcheck [OPTIONS] PRODUCT_NAME INVENTORY_PATH
```

**Arguments**:

* `PRODUCT_NAME`: The product name. e.g. qx, aqem, mx. The name has to exist in inventory as a key.  [required]
* `INVENTORY_PATH`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline deploy`

Deploy a pipeline from a given table name , product name or a directory taking into account the execution plan.
It can run the deployment in parallel or sequential.
Four approaches are possible:
1. Deploy from a given table name
2. Deploy from a product name
3. Deploy from a directory
4. Deploy from a table list file name

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
* `--exclude-table-file-name TEXT`: The file containing the list of tables to exclude from the deployment.
* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will create a pool.
* `--dml-only / --no-dml-only`: By default the deployment will do DDL and DML, with this flag it will deploy only DML  [default: no-dml-only]
* `--may-start-descendants / --no-may-start-descendants`: The children deletion will be done only if they are stateful. This Flag force to drop table and recreate all (ddl, dml)  [default: no-may-start-descendants]
* `--force-ancestors / --no-force-ancestors`: When reaching table with no ancestor, this flag forces restarting running Flink statements.  [default: no-force-ancestors]
* `--cross-product-deployment / --no-cross-product-deployment`: By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products.  [default: no-cross-product-deployment]
* `--dir TEXT`: The directory to deploy the pipeline from. If not provided, it will deploy the pipeline from the table name.
* `--parallel / --no-parallel`: By default the deployment will deploy the pipeline in parallel. This flag will deploy the pipeline in parallel.  [default: no-parallel]
* `--max-thread INTEGER`: The maximum number of threads to use when deploying the pipeline in parallel.  [default: 1]
* `--pool-creation / --no-pool-creation`: By default the deployment will not create a compute pool per table. This flag will create a pool.  [default: no-pool-creation]
* `--help`: Show this message and exit.

### `pipeline build-execution-plan`

From a given table, this command goes all the way to the full pipeline and assess the execution plan taking into account parent, children
and existing Flink Statement running status. It does not deploy. This is a command for analysis.

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
* `--exclude-table-file-name TEXT`: The file containing the list of tables to exclude from the deployment.
* `--compute-pool-id TEXT`: Flink compute pool ID to use as default.
* `--dml-only / --no-dml-only`: By default the deployment will do DDL and DML, with this flag it will deploy only DML  [default: no-dml-only]
* `--may-start-descendants / --no-may-start-descendants`: The descendants will not be started by default. They may be started differently according to the fact they are stateful or stateless.  [default: no-may-start-descendants]
* `--force-ancestors / --no-force-ancestors`: This flag forces restarting running ancestorsFlink statements.  [default: no-force-ancestors]
* `--cross-product-deployment / --no-cross-product-deployment`: By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products when considering descendants only.  [default: no-cross-product-deployment]
* `--help`: Show this message and exit.

### `pipeline report-running-statements`

Assess for a given table, what are the running dmls from its descendants. When the directory is specified, it will report the running statements from all the tables in the directory.

**Usage**:

```console
$ pipeline report-running-statements [OPTIONS] [INVENTORY_PATH]
```

**Arguments**:

* `[INVENTORY_PATH]`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; default: /Users/jerome/Documents/Code/customers/mc/data-platform-flink/pipelines]

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

* `[INVENTORY_PATH]`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; default: /Users/jerome/Documents/Code/customers/mc/data-platform-flink/pipelines]

**Options**:

* `--table-name TEXT`: The sink table name from where the undeploy will run.
* `--product-name TEXT`: The product name to undeploy from
* `--no-ack / --no-no-ack`: By default the undeploy will ask for confirmation. This flag will undeploy without confirmation.  [default: no-no-ack]
* `--cross-product / --no-cross-product`: By default the undeployment will process tables from the same product (valid with product-name). This flag allows to undeploy tables from different products.  [default: no-cross-product]
* `--compute-pool-id TEXT`: Flink compute pool ID to use as default.
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

### `pipeline analyze-pool-usage`

Analyze compute pool usage and assess statement consolidation opportunities.

This command will:
- Analyze current usage across compute pools (optionally filtered by product or directory)
- Identify running statements in each pool
- Assess opportunities for statement consolidation
- Generate optimization recommendations using simple heuristics

Examples:
    # Analyze all pools
    shift-left pipeline analyze-pool-usage

    # Analyze for specific product
    shift-left pipeline analyze-pool-usage --product-name saleops

    # Analyze for specific directory
    shift-left pipeline analyze-pool-usage --directory /path/to/facts/saleops

    # Combine product and directory filters
    shift-left pipeline analyze-pool-usage --product-name saleops --directory /path/to/facts

**Usage**:

```console
$ pipeline analyze-pool-usage [OPTIONS] [INVENTORY_PATH]
```

**Arguments**:

* `[INVENTORY_PATH]`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES]

**Options**:

* `-p, --product-name TEXT`: Analyze pool usage for a specific product only
* `-d, --directory TEXT`: Analyze pool usage for pipelines in a specific directory
* `--help`: Show this message and exit.
