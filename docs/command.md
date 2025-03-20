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

* `project`
* `table`
* `pipeline`

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

* `--project-type TEXT`: [default: data_product]
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
* `find-table-users`: Find the Flink Statements user of a given...
* `validate-table-names`: Go over the pipeline folder to assess...
* `update-tables`: Update the tables with SQL code changes...
* `unit-test`: Run all the unit tests or a specified test...

### `table init`

Build a new table structure under the specified path. For example to add a source table structure use for example the command:
`shift_left table init src_table_1 $PIPELINES/sources/p1`

**Usage**:

```console
$ table init [OPTIONS] TABLE_NAME TABLE_PATH
```

**Arguments**:

* `TABLE_NAME`: Table name to build  [required]
* `TABLE_PATH`: Path in which the table folder stucture will be created under.  [required]

**Options**:

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

Search the parent for a given table from the source project.

**Usage**:

```console
$ table search-source-dependencies [OPTIONS] TABLE_SQL_FILE_NAME SRC_PROJECT_FOLDER
```

**Arguments**:

* `TABLE_SQL_FILE_NAME`: [required]
* `SRC_PROJECT_FOLDER`: [env var: SRC_FOLDER; required]

**Options**:

* `--help`: Show this message and exit.

### `table migrate`

Migrate a source SQL Table defined in a sql file with AI Agent to a Staging area to complete the work.

**Usage**:

```console
$ table migrate [OPTIONS] TABLE_NAME SQL_SRC_FILE_NAME TARGET_PATH
```

**Arguments**:

* `TABLE_NAME`: the name of the table once migrated.  [required]
* `SQL_SRC_FILE_NAME`: the source file name for the sql script to migrate.  [required]
* `TARGET_PATH`: the target path where to store the migrated content (default is $STAGING)  [env var: STAGING; required]

**Options**:

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

### `table find-table-users`

Find the Flink Statements user of a given table

**Usage**:

```console
$ table find-table-users [OPTIONS] TABLE_NAME PIPELINE_PATH
```

**Arguments**:

* `TABLE_NAME`: The name of the table to search   [required]
* `PIPELINE_PATH`: Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `table validate-table-names`

Go over the pipeline folder to assess table name and naming convention are respected.

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
* `--class-to-use TEXT`: [default: typing.Annotated[str, &lt;typer.models.ArgumentInfo object at 0x1197de2d0&gt;]]
* `--help`: Show this message and exit.

### `table unit-test`

Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results

**Usage**:

```console
$ table unit-test [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: Name of the table to unit tests.  [required]

**Options**:

* `--test-case-name TEXT`: Name of the individual unit test to run.   [required]
* `--help`: Show this message and exit.

## `pipeline`

**Usage**:

```console
$ pipeline [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `build-metadata`: Build a pipeline from a sink table: add or...
* `delete-metadata`: Delete a pipeline definitions from a given...
* `build-all-metadata`: Go to the hierarchy of folders for...
* `report`: Generate a report showing the pipeline...
* `deploy`: Deploy a pipeline from a given folder

### `pipeline build-metadata`

Build a pipeline from a sink table: add or update {} each table in the pipeline

**Usage**:

```console
$ pipeline build-metadata [OPTIONS] DML_FILE_NAME PIPELINE_PATH
```

**Arguments**:

* `DML_FILE_NAME`: The path to the DML file. e.g. $PIPELINES/table-name/sql-scripts/dml.table-name.sql  [required]
* `PIPELINE_PATH`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline delete-metadata`

Delete a pipeline definitions from a given folder path

**Usage**:

```console
$ pipeline delete-metadata [OPTIONS] PATH_FROM_WHERE_TO_DELETE
```

**Arguments**:

* `PATH_FROM_WHERE_TO_DELETE`: [required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline build-all-metadata`

Go to the hierarchy of folders for dimensions, views and facts and build the pipeline definitions for each table found using recurring walk through

**Usage**:

```console
$ pipeline build-all-metadata [OPTIONS] PIPELINE_PATH
```

**Arguments**:

* `PIPELINE_PATH`: Pipeline path, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline report`

Generate a report showing the pipeline hierarchy for a given table using its pipeline_definition.json

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
* `--help`: Show this message and exit.

### `pipeline deploy`

Deploy a pipeline from a given folder

**Usage**:

```console
$ pipeline deploy [OPTIONS] TABLE_NAME INVENTORY_PATH
```

**Arguments**:

* `TABLE_NAME`: The table name containing pipeline_definition.json.  [required]
* `INVENTORY_PATH`: Path to the inventory folder, if not provided will use the $PIPELINES environment variable.  [env var: PIPELINES; required]

**Options**:

* `--compute-pool-id TEXT`: Flink compute pool ID. If not provided, it will create a pool.  [env var: CPOOL_ID; required]
* `--help`: Show this message and exit.
