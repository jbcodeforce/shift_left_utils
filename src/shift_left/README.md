# A shift left project management CLI

[See the shift_left documentation](https://jbcodeforce.github.io/shift_left_utils/). This section is to keep information for developing the CLI and core functions.

## Settings

* install [uv](https://docs.astral.sh/uv) and [pytest](https://docs.pytest.org/en/latest/)
* For testing with examples content, define the following environment variables from the project folder.

```sh
export PIPELINES=$(pwd)/examples/flink-project/pipelines
export CONFIG_FILE=$(pwd)/config.yaml
```

## Code structure

The code is in 3 layers

* The CLI module for each entities managed: table, project, pipeline
* The core modules for managing the entities, in a service oriented way.
* The utils modules with common functions to be used in the core modules

## Run unit tests

To avoid redundant tests, the tests are grouped in 3 sets:


* The CLI modules tests: will test the CLI functions and the delegate functions from core
* The core modules tests to test functions not addressed by CLI tests
* The utils modules tests to test function not addressed by CLI or core tests

The test should supports debugging and test in terminal, so all file access are relative to
the test class.

The tests includes a data folder with a src_project folder to represent a SQL or dbt project to migrate and a flink-project representing what can be migrated. 

Tests are executed in a virtual environment with python 3 and pytest.

```sh
pytest -s tests/core/test_table_mgr.py
pytest -s tests/core/test_project_mgr.py
pytest -s tests/core/test_pipeline_mgr.py
```


It is also possible to use uv and pytest as

```sh
uv run pytest -s tests/core/test_pipeline_mgr.py
uv run pytest -s tests/core/test_project_mgr.py
```

* Test the CLIs

```sh
uv run pytest -s tests/cli/test_project_cli.py
```

## Test CLI with python

```sh
python src/shift_left/cli.py pipeline build-metadata $PIPELINES/facts/p1/fct_order $PIPELINES
```

## Debug core functions

```sh
```


## Package CLI

* To install the tool locally with uv

```sh
uv tool install . -e  
```

* To build the wheel packaging

```sh
uv build
```