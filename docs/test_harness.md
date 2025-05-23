# Testing Flink Statement in isolation

???- info "Version"
    Created Mars 21- 2025 

The goals of this chapter is to present the requirements, design and how to use a test harness tool for Flink statement validation in the context of Confluent Cloud Flink. The tool is packaged as a command in the `shift-left` CLI.

```sh
shift_left table --help
```


## Context


We should differentiate two types of testing: Flink statement developer testing, like unit / component tests, and integration tests with other tables and with real data streams.

The objectives of a test harness for developer and system testers, is to validate the quality of a new Flink SQL statement deployed on Confluent Cloud and therefore address the following needs:

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


## Usage / Recipe

* Select a table to test the logic. This test tool is relevant for DML with complex logic. In this example `user_role` has a join between three tables: `src_p3_users`, `src_p3_tenants`, `src_p3_roles`:
  ```sql
  INSERT INTO int_p3_user_role
  WITH
      users as (
          select user_id,
                tenant_id,
                role_id,
                status
          from src_p3_users 
          left join src_p3_tenants on src_p3_users.tenant_id = src_p3_tenants.id
      ),
      roles as (
          select role_id
                role_name,
                u.tenant_id,
                u.user_id,
                u.status
          from src_p3_roles
          left join users u on src_p3_roles.role_id = u.role_id
      )
  SELECT * FROM roles;
  ```

* Verify the ddl and dml of the table are created under `sql-scripts`, verify the table inventory exists and is up-to-date, if not run `shift_left table build-inventory $PIPELINES`, then run the following command:
  ```sh
  shift_left table init-unit-tests <table_name>
  # example with the user_role (using the naming convention)
  shift_left table init-unit-tests int_p3_user_role
  ```

* In the table folder, under the pipelines, verify the created files under the `tests` folder. For each table, inputs to the dml under test, there will be a ddl script file created with the numbered suffix to match the unit test it supports.  In the example below the `dml_user_role.sql` has 3 input tables: `src_p3_users`, `src_p3_tenants`, `src_p3_roles`. For each of those input tables a foundation ddl is created to create the table with "_ut" suffix, (this is used for test isolation): `ddl_src_p3_roles.sql`, `ddl_src_p3_users.sql` and `ddl_src_p3_tenants.sql`
  ```sh
  user_role
  ├── Makefile
  ├── sql-scripts
  │   ├── ddl.int_p3_user_role.sql
  │   └── dml.int_p3_user_role.sql
  ├── tests
  │   ├── ddl_src_p3_roles.sql
  │   ├── ddl_src_p3_tenants.sql
  │   ├── ddl_src_p3_users.sql
  │   ├── insert_src_p3_roles_1.sql
  │   ├── insert_src_p3_roles_2.csv
  │   ├── insert_src_p3_tenants_1.sql
  │   ├── insert_src_p3_tenants_2.csv
  │   ├── insert_src_p3_users_1.sql
  │   ├── insert_src_p3_users_2.csv
  │   ├── test_definitions.yaml
  │   ├── validate_int_p3_user_role_1.sql
  │   └── validate_int_p3_user_role_2.sq
  ```

Then 2 test cases are created as you can see in the test_definitions.yaml
  ```yaml
  test_suite:
  - name: test_int_p3_user_role_1
    inputs:
    - table_name: src_p3_roles
      file_name: ./tests/insert_src_p3_roles_1.sql
      file_type: sql
    - table_name: src_p3_users
      file_name: ./tests/insert_src_p3_users_1.sql
      file_type: sql
    - table_name: src_p3_tenants
      file_name: ./tests/insert_src_p3_tenants_1.sql
      file_type: sql
    outputs:
    - table_name: int_p3_user_role
      file_name: ./tests/validate_int_p3_user_role_1.sql
      file_type: sql
  - name: test_int_p3_user_role_2
    inputs:
    - table_name: src_p3_roles
      file_name: ./tests/insert_src_p3_roles_2.csv
      file_type: csv
    - table_name: src_p3_users
      file_name: ./tests/insert_src_p3_users_2.csv
      file_type: csv
    - table_name: src_p3_tenants
      file_name: ./tests/insert_src_p3_tenants_2.csv
      file_type: csv
    outputs:
    - table_name: int_p3_user_role
      file_name: ./tests/validate_int_p3_user_role_2.sql
      file_type: sql
  ```

The two test cases use different approach to define the data: SQL or CSV files. This is a more flexible solution, so we can inject data, as csv rows, from an extract of data coming from kafka topic.

* Data engineers update the content of the insert statements and the validation statements. Once done, try unit testing with the command:
  ```sh
  shift_left table  run-test-suite <table_name> --test-case-name test_<table_name>_1 
  ```

A test execution may take some time as it performs the following steps:

1. Read the test definition.
1. Execute the ddl for the input tables with '_ut' suffix to keep specific test data.
1. Insert records in input tables.
1. Create a new output table with '_ut' suffix.
1. Deploy the DML to test
1. Deploy the validate SQL for each test case.
1. Build test report

* Run the complete suite of tests:
  ```sh
  shift_left table  run-test-suite <table_name>
  ```

* Clean the tests artifacts created on Confluent Cloud with the command:
  ```sf
  shift_left table delete-tests <table_name>
  ```




