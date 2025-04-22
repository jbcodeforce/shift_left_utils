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


## Integration tests

## Developer's notes

The modules to support the management of pipeline is `pipeline_mgr.py` and `deployment_mgr.py`.

* Testing a Flink deployment see [test - ]


For deployment the approach is to build a graph from the table developer want to deploy. The graph includes the parents and then the children. The graph is built reading static information about the relationship between statement, and then go over each statement and assess if for this table the dml is running. For a parent it does nothing

## Logs

## Troubleshooting

