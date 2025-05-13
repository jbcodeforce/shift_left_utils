# Testing Flink Statement in isolation

???- info "Version"
    Created Mars 21- 2025 

The goals of this chapter is to present the requirements and design of this test tool, which in included as a command in the `shift-left` CLI.

## Context


We should differentiate two types of testing: Flink statement developer testing, like unit / component tests, and integration tests with other tables and with real data streams.

The objectives of a test harness for developer and system integration is to validate the quality of a new Flink SQL statement deployed on Confluent Cloud (or Flink managed service) and therefore to address the following needs:

1. be able to deploy a flink statement (the ones we want to focus on are DMLs, or CTAS)
1. be able to generate test data from schema registry or table definition - and with developer being able to tune test data for each test cases.
1. produce test data to n source topics, consumes from the expected output topic and validates expected results. All this flow being one test case. This may be automated for non-regression testing to ensure continuous quality.
1. support multiple testcase definitions as a test suite
1. tear down topics and data.
1. Do not impact other topics that may be use to do integration tests within the same Kafka Cluster.

The following diagram illustrates the global infrastructure deployment context:

![](./images/test_frwk_infra.drawio.png)

The following diagram illustrates the target unit testing environment:

![](./images/test_frwk_design.drawio.png)


## Requirements

* [x] The command is integrated in the shilf-left CLI as:

```sh
table unit-test [OPTIONS] TABLE_NAME

Arguments:

TABLE_NAME: Name of the table to unit tests. [required]
Options:

--test-case-name TEXT: Name of the individual unit test to run. By default it will run all the tests [required]
--compute-pool-id TEXT: Flink compute pool ID. If not provided, it will create a pool. [env var: CPOOL_ID; required]
```

* [x] Be able to define test suite. The yaml is defined as

```yaml

foundations:
    - table_name: int_table_1
      ddl_for_test: tests/ddl_int_table_1.sql
    - table_name: int_table_2
      ddl_for_test: tests/ddl_int_table_2.sql
test_suite:
- name: test_case_1
    inputs:
    - table_name: int_table_1
    sql_file_name: tests/insert_int_table_1_1.sql
    - table_name: int_table_2
    sql_file_name: tests/insert_int_table_2_1.sql
    outputs:
    - table_name: fct_order
    sql_file_name: tests/validate_fct_order_1.sql
- name: test_case_2
    inputs:
    - table_name: int_table_1
      sql_file_name: tests/insert_int_table_1_2.sql
    - table_name: int_table_2
      sql_file_name: tests/insert_int_table_2_2.sql
    outputs:
    - table_name: fct_order
    sql_file_name: tests/validate_fct_order_2.sql
```

* [x] Organize tests under the tests folder of the table:

```sh
└── fct_order
    ├── Makefile
    ├── pipeline_definition.json
    ├── sql-scripts
    │   ├── ddl.fct_order.sql
    │   └── dml.fct_order.sql
    └── tests
        ├── ddl_int_table_1.sql
        ├── ddl_int_table_2.sql
        ├── insert_int_table_1_1.sql
        ├── insert_int_table_1_2.sql
        ├── insert_int_table_2_1.sql
        ├── insert_int_table_2_2.sql
        ├── test_definitions.yaml
        ├── validate_fct_order_1.sql
        └── validate_fct_order_2.sql
```

* [x] Create the above structure and template from the DML sql content at init phase. The command should be:

  ```sh
  table init-unit-tests table_name
  ```

  This loads table references and metadata and create content from sql_content.
  
* [ ] For each test cases defined do the following on a selected compute pool id

    * Execute the foundation sql statements to create the temporary tables for injecting test data (e.g. `tests/ddl_int_table_1.sql`, `tests/ddl_int_table_2.sql`). The name of the tables are changed to add `_ut` at the end to avoid colision with existing topics.
    * Deploy the target flink deployment to test, by loading the sql content from the file (e.g. `sql-scripts/dml.fct_order.sql`) but modifying the table names dynamically (e.g.  `int_table_1` becoming `int_table_1_ut`). 
    * Execute the inputs SQL statements to inject the data to the different source tables.
    * Execute the validation SQL statement to validate the expected results

* [ ] When test suite is done teardown the temporary tables.

## Usage/Demonstration

