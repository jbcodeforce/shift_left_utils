# A shift left project management CLI

[See the shift_left documentation](https://jbcodeforce.github.io/shift_left_utils/). This section is to keep information for developing the CLI and core functions.

## Settings

* install [uv](https://docs.astral.sh/uv) and [pytest](https://docs.pytest.org/en/latest/)
* For testing with examples content, define the following environment variables from the project folder.

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

```sh
python src/shift_left/cli.py pipeline build-metadata $PIPELINES/facts/p1/fct_order $PIPELINES
```

## Debug core functions



## Package CLI

* To install the tool locally with uv

```sh
uv tool install . -e  
```

* To build the wheel packaging

```sh
uv build
```