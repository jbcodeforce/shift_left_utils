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

### Release management

The script (`build_and_release.sh`) automates the CLI build and release branch creation process following the following instructions.

1. **Creates release branch**: When code is ready, create a new release branch off the `develop` branch. Run all tests and fixes on this release branch until ready for release.from `develop` (e.g., `v0.1.34`)
2. **Updates version numbers** in:
   - `src/shift_left/shift_left/cli.py`
   - `src/shift_left/pyproject.toml`
3. **Builds the wheel package** using `uv build .`
4. **Cleans old wheels** (keeps last 10 wheel and tar.gz files)
5. **Updates CHANGELOG.md** with recent commits from `main..develop`
6. **Commits all changes** with descriptive commit message to the release branch
7. **Validate non-regression tests**
8. **Merge to main**
9. **Tag the release**
10. **Merge back to develop**
11. **Create GitHub release** referencing the new tag
12. **Delete release branch**

#### Prerequisites

- **uv** package manager installed ([installation guide](https://docs.astral.sh/uv/getting-started/installation/))
- Git repository with `main` and `develop` branches
- Current working directory should be the `shift_left_utils` root directory

#### Usage

```bash
./build_and_release.sh <version>
# Example
./build_and_release.sh 0.1.34
```

#### What You Need to Do Manually After Running the Script

The script will provide you with the exact commands to run next:

1. **Test the release branch** and make any necessary fixes (see the runRegressionTest.sh in src/shift_left)
2. **Merge to main:**
   ```bash
   git checkout main
   git merge --no-ff v0.1.34
   git push
   ```
3. **Tag the release:**
   ```bash
   git tag -a 0.1.34 -m "Release version 0.1.34"
   git push origin 0.1.34
   ```
4. **Merge back to develop:**
   ```bash
   git checkout develop
   git merge --no-ff v0.1.34
   git push
   ```
5. **Create GitHub release** referencing the new tag, in the [git web page](https://github.com/jbcodeforce/shift_left_utils/releases), copy, paste the 0.1.33 section of the changelog to the release note.
6. **Delete release branch:**
   ```bash
   git branch -d v0.1.34
   ```

**Files Modified by the Script**
- `src/shift_left/shift_left/core/utils/app_config.py` - Updates `__version__` variable
- `src/shift_left/pyproject.toml` - Updates project version
- `CHANGELOG.md` - Adds new version entry with recent commits
- `src/shift_left/dist/` - Builds new wheel and cleans old ones


## Environment set up for developers

We are using [uv](https://docs.astral.sh/uv/) as a new Python package manager. See [uv installation documentation](https://docs.astral.sh/uv/getting-started/installation/) then follow the next steps to set your environment for development:

* Fork the `https://github.com/jbcodeforce/shift_left_utils` repo in your github account.
* Clone your repo to your local computer.
* Add the upstream repository: see guide for step 1-3 here: [forking a repo](https://help.github.com/articles/fork-a-repo/)
* Verify you have [set up the pre-requisited](./setup.md#pre-requisites)
1. Install [uv](https://github.com/astral-sh/uv) for Python package management
    ```sh
    # Install uv if not already installed
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # To update existing version
    uv self update
    ```

1. **Python Environment**: Ensure Python 3.12+ and create a virtual environment
   ```bash
   uv venv --python 3.12.0
   source .venv/bin/activate  # On Windows WSL: .venv\Scripts\activate
   ```

1. **Install Dependencies**: Use `uv` package manager (recommended)
   ```bash
   cd src/shift_left
   # Install project dependencies
   uv sync
   ```

* Define a config.yaml file to keep the important parameters of the CLI. 
	```sh
	cp src/shift_left/shift_left/core/templates/config_tmpl.yaml ./config.yaml
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
export CONFIG_FILE=shift_left_utils/src/shift_left/tests/config.yaml
```

* **Install shift_left Tool**:
   ```bash
   # under src/shift_left
   ls -al dist/
   # select the last version, for example:
   uv tool install dist/shift_left-0.1.28-py3-none-any.whl
   # Verify installation
   shift_left --help
   shift_left version
   ```

    You can also use `pip` if you have an existing Python environment:
    ```sh
    pip install src/shift_left/dist/shift_left-0.1.28-py3-none-any.whl
    ```

* Make sure you are logged into Confluent Cloud and have defined at least one compute pool.

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
 uv run shift_left
```

* It is also possible to test the CLI with python:

```sh
uv run shift_left pipeline build-metadata $PIPELINES/facts/p1/fct_order $PIPELINES
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

To avoid comflict between test execution, each test gets its own copy of data into a temporary folder defined in the pytest fixture in `ut/core/conftest.py`. Tests can run simultaneously without conflicts. The temporary directories are automatically removed after tests.

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
    uv tool install shift_left@dist/shift_left-0.1.46-py3-none-any.whl
    ```

* To Build the [docs/command.md](./command.md) documentation from the code run:
    ```sh
    # under the shift_left_utils/src/shift_left folder
    ./updateDoc.sh
	# same as
	uv run typer shift_left/cli.py utils docs --output ../../docs/command.md

    ```

* Recompile a requirements.txt for pip users:
    ```sh
    uv pip compile pyproject.toml -o requirements.txt
    ```

### Publish to PyPI

From `src/shift_left`, build then publish only the current version to avoid uploading older wheels in `dist/`:

```sh
cd src/shift_left
uv build .
VERSION=$(grep '^version' pyproject.toml | cut -d'"' -f2)
UV_PUBLISH_TOKEN=pypi-xxxxxxxx uv publish dist/shift_left-${VERSION}*
```
