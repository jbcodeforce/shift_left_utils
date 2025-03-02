# A shift left project management CLI

[See the product documentation]()

## Settings

For testing with examples content define the following environment variables from the project folder.

```sh
export PIPELINES=$(pwd)/examples/flink-project/pipelines
export CONFIG_FILE=$(pwd)/config.yaml
```

## Run unit tests

Using uv and pytest:

* Test different modules

```sh
uv run pytest -s tests/core/test_pipeline_mgr.py
uv run pytest -s tests/core/test_project_mgr.py
```

* Test the CLIs

```sh
uv run pytest -s tests/cli/test_project_cli.py
```

## Test CLI with python


## Package CLI

* To install the tool locally with uv

```sh
uv tool install . -e  
```

* To build the wheel packaging

```sh
uv build
```