# Code structure and development practices

This chapter is about code design, code organization and testing it. The packaging of the set of tools is now a CLI done with Python [typer]()

## The components

The CLI includes three main section: 1/ project, 2/ table and 3/ pipeline to cover the main set of commands used during a Flink project.

![](./images/component-view.drawio.png)

The green components are the one exposing features and commands to the end user via the CLI. The set of commands are documented [here](../command.md). This command documentation is created automatically.

The white components are a set of services to support the end-user facing commands and to offer functions to other services. They try to group concerns related to the following resources:

* project
* table
* pipeline
* Flink Statement
* Confluent Cloud compute pools
* Tests for test harness
* Deployment to manage execution plan.

Finally a set of lower level utilies are used to do some integration with local files and remote REST API.

The clear separation of concern is still under refactoring.

## Code organization

## Unit testing

All test cases are under tests/ut and executed with

```sh
uv run pytest -s tests/ut
```

## Integration tests


All test cases are under tests/it and executed with

```sh
uv run pytest -s tests/it
```

## Classical scenarios

Be source to set environment variables for testing: 

```sh
export FLINK_PROJECT=./tests/data/flink-project
export PIPELINES=$FLINK_PROJECT/pipelines
export STAGING=$FLINK_PROJECT/staging
export SRC_FOLDER=./tests/data/dbt-project

export TOPIC_LIST_FILE=$FLINK_PROJECT/src_topic_list.txt 
export CONFIG_FILE=./tests/config.yaml
export CCLOUD_ENV_ID=env-
export CCLOUD_ENV_NAME=j9r-env
export CCLOUD_KAFKA_CLUSTER=j9r-kafka
export CLOUD_REGION=us-west-2
export CLOUD_PROVIDER=aws
export CCLOUD_CONTEXT=login-jboyer@confluent.io-https://confluent.cloud
export CCLOUD_COMPUTE_POOL_ID=lfcp-
```

### Work with SQL migration

1. Assess dependencies from the source project (see the tests/data/dbt-project folder) with the command:

    ```sh
    uv run python src/shift_left/cli.py table search-source-dependencies $SRC_FOLDER/facts/p7/fct_user_role.sql
    ```

1. Migrate a fact table and the others related ancestors to Staging using the table name that will be the folder name too.

    ```sh
    uv run python src/shift_left/cli.py table migrate user_role $SRC_FOLDER/facts/p7/fct_user_role.sql $STAGING --recursive
    ```

    The tool creates a folder in `./tests/data/flink-project/staging/facts/p7`, run the LLM , and generate migrate Flink SQL statement


### Work on pipeline deployment

1. Be sure the inventory is created: `uv run python src/shift_left/cli.py `

## Developer's notes

The modules to support the management of pipeline is `pipeline_mgr.py` and `deployment_mgr.py`.

* Testing a Flink deployment see [test - ]


For deployment the approach is to build a graph from the table developer want to deploy. The graph includes the parents and then the children. The graph is built reading static information about the relationship between statement, and then go over each statement and assess if for this table the dml is running. For a parent it does nothing

## Logs

All the logs are under $HOME/.shift_left/logs folder.

## Troubleshooting

