# Contribute to this repository

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

We really value contributions, and to maximize the impact of code contributions, we request that any contributions follow the guidelines below.  If you are new to open source contribution and would like some more pointers or guidance, you may want to check out [**Your First PR**](http://yourfirstpr.github.io/) and [**First Timers Only**](https://www.firsttimersonly.com/).  These are few projects that help on-board new contributors to the overall process.

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

The following core principles for the management of this code is using the `gitflow` process with separate `main` and `develop` branches for a structured release process. 

* **main Branch**: This branch always reflects the production-ready, stable code. Only thoroughly tested and finalized code is merged into `main`. Commits are tagged in `main` with version numbers for easy tracking of releases.
* **develop Branch**: This branch serves as the integration point for all new features and ongoing development. Feature branches are created from `develop` and merged back into it after completion.
* Creating a **Release Branch**: When a set of features in develop is deemed ready for release, a new release branch is created from `develop`. This branch allows for final testing, bug fixes, and release-specific tasks without interrupting the ongoing development work in develop.
* **Finalizing the Release**: Only bug fixes and necessary adjustments are made on the `release` branch. New feature development is strictly avoided.
* **Merging and Tagging**: Once the release branch is stable and ready for deployment, it's merged into two places:

    * `main`: The release branch is merged into main, effectively updating the production-ready code with the new release.
    * `develop`: The release branch is also merged back into develop to ensure that any bug fixes or changes made during the release preparation are incorporated into the ongoing development work.

* **Tagging**: After merging into main, the merge commit is tagged with a version number (e.g., v1.0.0) to mark the specific release point in the repository's history.
* **Cleanup**: After the release is finalized and merged, the release branch can be safely deleted

#### git commands for the different development tasks

1. Create a `feature` branch from  the `develop` branch
    ```sh
    git checkout -b  feature_a develop
    ```

1. Do your work:
    - Write your code
    - Write your unit tests and potentially your integration tests
    - Pass your tests locally
    - Commit your intermediate changes as you go and as appropriate to your `feature` branch
    - Repeat until satisfied

1. Create a pull request against the same targeted upstream `develop` branch.

    [Creating a pull request](https://help.github.com/articles/creating-a-pull-request/)

???- info "Forked repository special treatment"
    If you forked the main repository, once the pull request has been reviewed, accepted and merged into the `develop` branch, you should synchronize your remote and local forked github repository `main` branch with the upstream main branch. To do so:

    * Pull to your local forked repository the latest changes upstream (that is, the pull request).
        ```sh
        git pull upstream main
        ```

    * Push those latest upstream changes pulled locally to your remote forked repository.
        ```sh
        git push origin main
        ```

**Release management:**

1. When code is ready, create a new release branch off the `develop` branch. Run all tests and fixes on this release branch until ready for release.
    ```sh
    git checkout -b  v0.1.28 develop
    ```
1. Modify the cli.py to change the version number so `shift_left version` will return the matching version. Change the `pyproject.toml` version number and build the wheel (see [below section](#build)). Remove the `.gitignore` under the dist folder and remove the older wheel (may be keep the last 10).

1. Merge into main:
    ```bash
    git checkout main
    git merge --no-ff v0.1.28
    ```
1. Tag the main branch: 
    ```sh
    git tag -a 0.1.28 -m "Release version 0.1.28"
    git push origin 0.1.28
    ```
1. Merge into the `develop` branch:
    ```bash
    git checkout develop
    git merge --no-ff v0.1.28
    ```
1. In gihub, create a new release referencing the newly created tag
1. Delete the release branch: 
    ```sh
    git branch -d v0.1.28
    ```


## Environment set up for developers

We are using [uv](https://docs.astral.sh/uv/) as a new Python package manager. See [uv installation documentation](https://docs.astral.sh/uv/getting-started/installation/) then follow the next steps to set your environment for development:

* Fork the `https://github.com/jbcodeforce/shift_left_utils` repo in your github account.
* Clone your repo to your local computer.
* Add the upstream repository: see guide for step 1-3 here: [forking a repo](https://help.github.com/articles/fork-a-repo/)
* Verify you have [set up the pre-requisited](./setup.md#pre-requisites) with the below differences:
    * Create a new virtual environment in any folder, but could be the : `uv venv`

* Activate the environment:

```sh
source .venv/bin/activate
```
* Define a config.yaml file to keep the important parameters of the CLI. 

```sh
cp src/shift_left/src/shift_left/core/templates/config_tmpl.yaml ./config.yaml
```

* Get the credentials for the Confluent Cloud Kafka cluster and Flink compute pool, modify the config.yaml file

???- info "the structure of the config.yaml"
    This file is to keep configuration to access Confluent Cloud resources. It is grouped in 5 parts: 
    
    * kafka to access Kafka cluster
    * confluent_cloud to manage resources at the organization level
    * flink: to manage Statement and compute pools
    * app: to define CLI parameters
    * registry: to access schema registry (not used)

    The Flink, and Confluent Cloud api keys and secrets are different.

* Set the CONFIG_FILE environment variable to point to the config.yaml file. The following is used for integration tests.

```sh
export CONFIG_FILE=shift_left_utils/src/shift_left/tests/config-ccloud.yaml
```

## Development activities

See [the code explanation and design approach chapter.](./coding/index.md)


### Install dependencies and the cli as uv tool

* Install dependencies and tool for iterative development, under the `src/shift_left` folder (the one with the `uv.lock`  and `pyproject.toml` files)
    ```sh
    uv tool list
    uv tool install .
    ```

* Verify it is installed locally (version will differ)
    ```sh
    uv tool list 
    shift-left v0.1.28
    ```

* Uninstall:
    ```sh
    uv tool list
    uv tool uninstall shift_left
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
uv run pytest -s tests/ut/core/test_project_mgr.py
uv run pytest -s tests/ut/core/test_pipeline_mgr.py
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

* Rebuild the CLI as a wheel packaging: 
    ```sh
    uv build .
    ``` 
    under the src/shift_left project.

* Deploy the CLI from the wheel: 1/ first the last wheel number and then 2/ execute the commands like:
    ```sh
    uv tool uninstall shift_left
    uv tool install shift_left@dist/shift_left-0.1.19-py3-none-any.whl
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