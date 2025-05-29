# Testing Flink Statement in isolation

???- info "Version"
    Created Mars 21- 2025 

The goals of this chapter is to present the requirements, design and how to use a test harness tool for Flink statement validation in the context of Confluent Cloud Flink. The tool is packaged as a command in the `shift-left` CLI.

```sh
shift_left table --help

# three features are available:
init-unit-tests   Initialize the unit test folder and template files for a given table. It will parse the SQL statements to create the insert statements for the unit tests. It is using the table inventory to find the table folder for the given table name.
run-test-suite    Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results
delete-tests      Delete the Flink statements and kafka topics used for unit tests for a given table.
```

[See usage paragraph](#usage--recipe)

## Context


We should differentiate two types of testing: Flink statement developer's tests, like unit / component tests, and Flink statement integration tests which group multiple Flink statements and process real data streams.

The objectives of a test harness for developers and system testers, is to validate the quality of a new Flink SQL statement deployed on Confluent Cloud and therefore address the following needs:

1. be able to deploy a flink statement under test (the ones we want to focus on are DMLs, or CTAS)
1. be able to generate test data from table definition - and with developer being able to tune test data for each test cases.
1. produce test data for the n source tables, in SQL insert statements or csv file.
1. validate test result by looking at records in the output table(s) of the Flink statment under test and applying conditions on data to claim the test failed or succeed. 
1. the flow of defining input data and validation scripts is a test case. 
1. support multiple testcase definitions as a test suite. Test suite may be automated for non-regression testing to ensure continuous quality.
1. Once tests are completed, tear down tables and data.
1. Do not impact other tables that may be used to do integration tests within the same Kafka Cluster.

The following diagram illustrates the global infrastructure deployment context:

<figure markdown="span">
![1](./images/test_frwk_infra.drawio.png){ width=750 }
<figcaption>Test Harness environment and scope</figcaption>
</figure>



* One the left, the developer mahchine is used to run the test harness tool and send statements to Confluent cloud environment using the REST API. The Flink API key and secrets are used. 
* The Flink statement under test is the same as the one going to production, except the tool may change the name of the source table to use the '_ut' postfix. 
* The green cylenders represent Kafka Topics which are mapped to Flink tables. They are defined specifically by the tool.
* As any tables created view Flink on Confluent Cloud have schema defined in schema registry, then schema contest is used to avoid conflict within the same cluster. 

The following diagram illustrates a  target unit testing environment:

<figure markdown="span">
![2](./images/test_frwk_design.drawio.png){ width=750 }
<figcaption>Deployed Flink statements for unit testing</figcaption>
</figure>




## Usage / Recipe

* Select a table to test the logic from. This test tool is relevant for DML with complex logic. In this example `user_role` has a join between three tables: `src_p3_users`, `src_p3_tenants`, `src_p3_roles`:
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

* Verify the ddl and dml files for the selected table are defined under `sql-scripts`, verify the table inventory exists and is up-to-date, if not run `shift_left table build-inventory $PIPELINES`
* Initialize the test code by running the following command:
  ```sh
  shift_left table init-unit-tests <table_name>
  # example with the user_role (using the naming convention)
  shift_left table init-unit-tests int_p3_user_role
  ```

* For each input table, of the dml under test, there will be a ddl script file created with the numbered postfix to match the unit test it supports: `insert_src_p3_roles_1.sql` for inserting records to the table `src_p3_roles` in the context of test case # 1.  In the example below the `dml_user_role.sql` has 3 input tables: `src_p3_users`, `src_p3_tenants`, `src_p3_roles`. For each of those input tables, a foundation ddl is created to create the table with "_ut" postfix, (this is used for test isolation): `ddl_src_p3_roles.sql`, `ddl_src_p3_users.sql` and `ddl_src_p3_tenants.sql`
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

Then 2 test cases are created as you can see in the `test_definitions.yaml`
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

The two test cases use different approach to define the data: SQL or CSV files. This is a more flexible solution, so the tool can inject data, as csv rows. The data may come from an extract of kafka topic records.

* Data engineers update the content of the insert statements and the validation statements. Once done, try unit testing with the command:
  ```sh
  shift_left table  run-test-suite <table_name> --test-case-name test_<table_name>_1 
  ```

A test execution may take some time as it performs the following steps:

1. Read the test definition.
1. Execute the ddl for the input tables with '_ut' postfix to keep specific test data.
1. Insert records in input tables.
1. Create a new output table with '_ut' postfix.
1. Deploy the DML to test
1. Deploy the validation SQL for each test case.
1. Build test report


To run the complete suite of tests:
  ```sh
  shift_left table  run-test-suite <table_name>
  ```

Clean the tests artifacts created on Confluent Cloud with the command:
  ```sf
  shift_left table delete-tests <table_name>
  ```

## Running with more data

The second test cases created by the `shift_left table init-unit-tests ` command use a csv file to demonstrate how the tool can manage more data. It is possible to extract data from an existing topics, as json or csv content and then persist it as csv file. The tool, as of now, is transforming the rows in the csb file as insert value SQL. 

In the future it could direcly write to a Kafka topics that are the input tables for the dml under test.

Data engineers may use the csv format to create a lot of records. Now the challenge will be to define the validation SQL script, but this is another story.


