# shift_left  CLI

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
* `update-all-makefile`
* `list-topics`: Get the list of topics for the Kafka Cluster.

### `project init`

Create a project structure with a specified name, target path, and optional project type. 
The project type can be either &#x27;kimball&#x27; <span style="color: #000000; text-decoration-color: #000000">.</span>

**Usage**:

```console
$ project init [OPTIONS] [PROJECT_NAME] [PROJECT_PATH]
```

**Arguments**:

* `[PROJECT_NAME]`: [default: default_data_project]
* `[PROJECT_PATH]`: [default: ./tmp]

**Options**:

* `--project-type TEXT`: [default: data_product]
* `--help`: Show this message and exit.

### `project update-all-makefile`

**Usage**:

```console
$ project update-all-makefile [OPTIONS] PIPELINE_FOLDER_PATH
```

**Arguments**:

* `PIPELINE_FOLDER_PATH`: [required]

**Options**:

* `--help`: Show this message and exit.

### `project list-topics`

Get the list of topics for the Kafka Cluster. Be sure to have a conflig.yaml file setup

**Usage**:

```console
$ project list-topics [OPTIONS] PROJECT_PATH
```

**Arguments**:

* `PROJECT_PATH`: [required]

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
* `find-table-users`: Find the users of a given table

### `table init`

Build a new table structure under the specified path. For example to add a source table structure
use table init src_table_1 $PIPELINES/sources/p1

**Usage**:

```console
$ table init [OPTIONS] TABLE_NAME TABLE_PATH
```

**Arguments**:

* `TABLE_NAME`: [required]
* `TABLE_PATH`: [required]

**Options**:

* `--help`: Show this message and exit.

### `table build-inventory`

Build the table inventory from the PIPELINES path.

**Usage**:

```console
$ table build-inventory [OPTIONS] PIPELINE_PATH
```

**Arguments**:

* `PIPELINE_PATH`: [env var: PIPELINES; required]

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

* `TABLE_NAME`: [required]
* `PIPELINE_FOLDER_NAME`: [required]

**Options**:

* `--help`: Show this message and exit.

### `table find-table-users`

Find the users of a given table

**Usage**:

```console
$ table find-table-users [OPTIONS] TABLE_NAME
```

**Arguments**:

* `TABLE_NAME`: [required]

**Options**:

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
* `report`: Generate a report showing the pipeline...
* `deploy`: Deploy a pipeline from a given folder

### `pipeline build-metadata`

Build a pipeline from a sink table: add or update {} each table in the pipeline

**Usage**:

```console
$ pipeline build-metadata [OPTIONS] DML_FILE_NAME PIPELINE_PATH
```

**Arguments**:

* `DML_FILE_NAME`: [required]
* `PIPELINE_PATH`: [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline delete-metadata`

Delete a pipeline definitions from a given folder

**Usage**:

```console
$ pipeline delete-metadata [OPTIONS] PATH_FROM_WHERE_TO_DELETE
```

**Arguments**:

* `PATH_FROM_WHERE_TO_DELETE`: [required]

**Options**:

* `--help`: Show this message and exit.

### `pipeline report`

Generate a report showing the pipeline hierarchy for a given table using its pipeline_definition.json

**Usage**:

```console
$ pipeline report [OPTIONS] TABLE_NAME INVENTORY_PATH
```

**Arguments**:

* `TABLE_NAME`: The table name (folder name) containing pipeline_definition.json  [required]
* `INVENTORY_PATH`: Path to the inventory folder  [env var: PIPELINES; required]

**Options**:

* `--yaml`: Output the report in YAML format
* `--json`: Output the report in JSON format
* `--help`: Show this message and exit.

### `pipeline deploy`

Deploy a pipeline from a given folder

**Usage**:

```console
$ pipeline deploy [OPTIONS] TABLE_NAME INVENTORY_PATH
```

**Arguments**:

* `TABLE_NAME`: [required]
* `INVENTORY_PATH`: Path to the inventory folder  [env var: PIPELINES; required]

**Options**:

* `--help`: Show this message and exit.

