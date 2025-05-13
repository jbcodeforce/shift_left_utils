# Contributing to this repository

This chapter addresses how to support the development of this open source project.

Anyone can contribute to this repository and associated projects.

There are multiple ways to contribute: report bugs and suggest improvements, improve the documentation, and contribute code.

## Bug reports, documentation changes, and feature requests

If you would like to contribute to the project in the form of encountered bug reports, necessary documentation changes, or new feature requests, this can be done through the use of the repository's [**Issues**](https://github.com/jbcodeforce/shift_left_utils/issues) list.

Before opening a new issue, please check the existing list to make sure a similar or duplicate item does not already exist.  When you create your issues, please be as explicit as possible and be sure to include the following:

* **Bug reports**

    * Specific project version
    * Deployment environment
    * A minimal, but complete, setup of steps to recreate the problem

* **Documentation changes**

    * URL to existing incorrect or incomplete documentation (either in the project's GitHub repo or external product documentation)
    * Updates required to correct current inconsistency
    * If possible, a link to a project fork, sample, or workflow to expose the gap in documentation.

- **Feature requests**

    * Complete description of project feature request, including but not limited to, components of the existing project that are impacted, as well as additional components that may need to be created.
    * A minimal, but complete, setup of steps to recreate environment necessary to identify the new feature's current gap.

The more explicit and thorough you are in opening GitHub Issues, the more efficient your interaction with the maintainers will be.  When creating the GitHub issue for your bug report, documentation change, or feature request, be sure to add as many relevant labels as necessary (that are defined for that specific project).  These will vary by project, but will be helpful to the maintainers in quickly triaging your new GitHub issues.

## Code contributions

We really value contributions, and to maximize the impact of code contributions, we request that any contributions follow the guidelines below.  If you are new to open source contribution and would like some more pointers or guidance, you may want to check out [**Your First PR**](http://yourfirstpr.github.io/) and [**First Timers Only**](https://www.firsttimersonly.com/).  These are a few projects that help on-board new contributors to the overall process.

### Coding and Pull Requests best practices

* Please ensure you follow the coding standard and code formatting used throughout the existing code base.

    * This may vary project by project, but any specific diversion from normal language standards will be explicitly noted.

* One feature / bug fix / documentation update per pull request

    - Always pull the latest changes from upstream and rebase before creating any pull request.  
    - New pull requests should be created against the `integration` branch of the repository, if available.
    - This ensures new code is included in full-stack integration tests before being merged into the `main` branch

- All new features must be accompanied by associated tests.

    - Make sure all tests pass locally before submitting a pull request.
    - Include tests with every feature enhancement, improve tests with every bug fix

### Github and git flow

The internet is littered with guides and information on how to use and understand git.

However, here's a compact guide that follows the suggested workflow that we try to follow:

1. Fork the desired repo in github.

2. Clone your repo to your local computer.

3. Add the upstream repository

    Note: Guide for step 1-3 here: [forking a repo](https://help.github.com/articles/fork-a-repo/)

4. Create new development branch off the targeted upstream branch.  This will often be `main`.

    ```sh
    git checkout -b <my-feature-branch> main
    ```

5. Do your work:

   - Write your code
   - Write your tests
   - Pass your tests locally
   - Commit your intermediate changes as you go and as appropriate
   - Repeat until satisfied

6. Fetch latest upstream changes (in case other changes had been delivered upstream while you were developing your new feature).

    ```sh
    git fetch upstream
    ```

7. Rebase to the latest upstream changes, resolving any conflicts. This will 'replay' your local commits, one by one, after the changes delivered upstream while you were locally developing, letting you manually resolve any conflict.

    ```sh
    git branch --set-upstream-to=upstream/main
    git rebase
    ```

    Instructions on how to manually resolve a conflict and commit the new change or skip your local replayed commit will be presented on screen by the git CLI.

8. Push the changes to your repository

    ```sh
    git push origin <my-feature-branch>
    ```

9. Create a pull request against the same targeted upstream branch.

    [Creating a pull request](https://help.github.com/articles/creating-a-pull-request/)

Once the pull request has been reviewed, accepted and merged into the main github repository, you should synchronize your remote and local forked github repository `main` branch with the upstream main branch. To do so:

10. Pull to your local forked repository the latest changes upstream (that is, the pull request).

    ```sh
    git pull upstream main
    ```

11. Push those latest upstream changes pulled locally to your remote forked repository.

    ```sh
    git push origin main
    ```

### What happens next?

- All pull requests will be automatically built with GitHub Action, when implemented by that specific project.
  - You can determine if a given project is enabled for GitHub Action workflow by the existence of a `./github/workflow` folder in the root of the repository or branch.
  - When in use, unit tests must pass completely before any further review or discussion takes place.
- The repository maintainer will then inspect the commit and, if accepted, will pull the code into the upstream branch.
- Should a maintainer or reviewer ask for changes to be made to the pull request, these can be made locally and pushed to your forked repository and branch.
- Commits passing this stage will make it into the next release cycle for the given project.

## Environment set up

We are using [uv](https://docs.astral.sh/uv/) as a new Python package manager. See [uv installation documentation](https://docs.astral.sh/uv/getting-started/installation/) then follow the next steps to set your environment for development:

* Clone this repository: 

```sh
git clone  https://github.com/jbcodeforce/shift_left_utils.git
cd shift_left_utils
```

* Create a new virtual environment in any folder, but could be the : `uv venv`
* Activate the environment:

```sh
source .venv/bin/activate
```
* Define a config.yaml file to keep some important parameters of the CLI. 

```sh
cp src/shift_left/src/shift_left/core/templates/config_tmpl.yaml ./config.yaml
```

* Connect to Confluent Cloud with CLI, then get the environment and compute pool identifiers:

```sh
confluent login --save
```

* Get the credentials for the Confluent Cloud Kafka cluster and Flink compute pool, modify the config.yaml file

???- info "the structure of the config.yaml"
    This file is to keep configuration to access Confluent Cloud resources. It is grouped in 5 parts: 
    
    * kafka to access Kafka cluster
    * confluent_cloud to manage resources at the organization level
    * flink: to manage Statement and compute pools
    * app: to define CLI parameters
    * registry: to access schema registry

    The Flink, and Confluent Cloud api keys and secrets are different.

* Set the CONFIG_FILE environment variable to point to the config.yaml file. For running all the tests, the config.yaml file may be saved under the `tests` folder

```sh
export CONFIG_FILE=shift_left_utils/src/shift_left/tests/config.yaml
```

## Development activities

`uv` includes a dedicated interface for interacting with tools (cli programs). Tools can be invoked without installation using `uv tool run`

### Understand the code

The code is in three layers:

* The CLI module for each entities managed: table, project, pipeline
* The core modules for managing the entities, in a service oriented way.
* The utils modules with common functions to be used in the core modules

The cli.py is based of [typer module](https://typer.tiangolo.com/) and is the top level access to the cli execution.

```sh
└── shift_left
│       ├── cli.py
│       ├── cli_commands
│       │   ├── pipeline.py
│       │   ├── project.py
│       │   └── table.py
│       ├── core
│       │   ├── deployment_mgr.py
│       │   ├── flink_statement_model.py
│       │   ├── pipeline_mgr.py
│       │   ├── process_src_tables.py
│       │   ├── project_manager.py
│       │   ├── table_mgr.py
│       │   ├── templates
│       │   │   ├── common.mk
│       │   │   ├── config_tmpl.yaml
│       │   │   ├── create_table_skeleton.jinja
│       │   │   ├── dedup_dml_skeleton.jinja
│       │   │   ├── dml_src_tmpl.jinja
│       │   │   ├── makefile_ddl_dml_tmpl.jinja
│       │   │   ├── test_dedup_statement.jinja
│       │   │   └── tracking_tmpl.jinja
│       │   ├── test_mgr.py
│       │   └── utils
│       │       ├── app_config.py
│       │       ├── ccloud_client.py
│       │       ├── file_search.py
│       │       ├── flink_sql_code_agent_lg.py
│       │       ├── sql_parser.py
│       │       └── table_worker.py
```

### Install dependencies and the cli as uv tool:

* Install dependencies and tool for iterative development, under the `src/shift_left` folder (the one with the `uv.lock`  and `pyproject.toml` files)

```sh
uv tool install .
```

* Verify it is installed locally (version will differ)

```sh
uv tool list 
shift-left v0.1.1
- shift_left
```

(The version number is specified in `pyproject.toml` file)

### Testing

This section addresses the things to know for running tests for the CLI.

???- info "Test projects under data"
    The `tests/data` folder includes two test projects:

    1/ flink-project and 2/ dbt-project

    The `dbt-project` include a dbt project to migrate source SQL from. The flink-project is the outcome of the migration with data engineering tuning the SQL and deploy them with Make.

    Those projects are used for unit tests and even to explain how to use the CLI by end user.

* Be sure to set the PIPELINES environment variable to point to the `tests/data/flink-project/pipelines` (be sure to be under the `src/shift_left` folde): 

```sh
export PIPELINES=$(pwd)/tests/flink-project/pipelines
```

* Running the cli from python code, be sure to be under the `src/shift_left` folder and use:

```sh
 uv run src/shift_left/cli.py
```



* It is also possible to test the CLI with python:

```sh
python src/shift_left/cli.py pipeline build-metadata $PIPELINES/facts/p1/fct_order $PIPELINES
```

To avoid redundant tests, the tests are grouped in three sets:

1. The CLI modules tests: will test the CLI functions and the delegate functions from core
1. The core modules tests: to test functions not addressed by CLI tests
1. The utils modules tests: to test function not addressed by CLI or core tests

The test should supports debugging and test in Terminal, so all file accesses are relative to the test class.

Tests are executed in a virtual environment with python 3 and pytest.

```sh
uv run pytest -s tests/it/core/test_table_mgr.py
uv run pytest -s tests/core/test_project_mgr.py
uv run pytest -s tests/core/test_pipeline_mgr.py
```

* Test the CLIs

```sh
uv run pytest -s tests/cli/test_project_cli.py
```



### Debug core functions

Use the following settings for vscode based IDE

```json
    {
        "name": "Python Debugger: current file",
        "type": "debugpy",
        "request": "launch",
        "program": "${file}",
        "console": "integratedTerminal",
        "cwd": "${fileDirname}",
        "env": {
            "CONFIG_FILE": "${fileWorkspaceFolder}/config.yaml"
            },
    },
```

### Build

* To install the tool locally with uv and to support hot code update 

```sh
uv tool install . -e  
```

* Rebuild the CLI as a wheel packaging: `uv build .` under the src/shift_left project.
* Deploy the CLI from the wheel: 1/ first the last wheel number and then 2/ execute the commands like:

```sh
uv tool uninstall shift_left
uv tool install shift_left@dist/shift_left-0.1.4-py3-none-any.whl
```

* To Build the [Command.md](./command.md) documentation from the code run:

```sh
# under the shift_left_utils/src/shift_left folder
 uv run typer src/shift_left/cli.py utils docs --output ../../docs/command.md
```

* Recompile a requirements.txt for pip users:

```sh
uv pip compile pyproject.toml -o requirements.txt
```