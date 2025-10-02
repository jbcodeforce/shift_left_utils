# Testing Flink Statement in isolation

???- info "Version"
    Created March 21 - 2025 
    Updated Sept 24 -2025

The goals of this chapter is to present the requirements, design and how to use the shift_left test harness commands for Flink statement validation in the context of Confluent Cloud Flink. The tool is packaged as a command in the `shift-left` CLI.

```sh
shift_left table --help

# three features are available:
init-unit-tests   Initialize the unit test folder and template files for a given table. It will parse the SQL statements to create the insert statements for the unit tests. It is using the table inventory to find the table folder for the given table name.
run-unit-tests    Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results
delete-unit-tests      Delete the Flink statements and kafka topics used for unit tests for a given table.
```

[See usage paragraph](#unit-test-usage-and-recipes)

## Context

We should differentiate two types of testing: Flink statement developer's tests, like **unit testing** of one flink statement, and Flink statement **integration tests** which group multiple Flink statements and process data end-to-end.

The objectives of a test harness for developers and system testers, is to validate the quality of a new Flink SQL statement deployed on Confluent Cloud for Flink and therefore address the following needs:

1. be able to deploy a unique flink statement under test (the ones we want to focus on are DMLs, or CTAS)
1. be able to generate test data from the table definition and DML script content - with the developers being able to tune generated test data for each test cases.
1. produce synthetic test data for the n source tables using SQL insert statements or via csv files.

    <figure markdown="span">
    ![1](./images/test_frwk.drawio.png)
    </figure>

1. validate test result by looking at records in the output table(s) of the Flink statement under test and applying conditions on data to claim the test failed or succeed. As an example:
  ```sql
  with result_table as (
    select * from fct_orders
    where id = 'order_id_1' and account_name = 'account of bob'
  ) 
  SELECT CASE WHEN count(*)=1 THEN 'PASS' ELSE 'FAIL' END from result_table;
  ```

1. the flow of defining input data and validation scripts is a test case. The following Yaml definition, define one test with input and output SQL references:
  ```yaml
  - name: test_p5_dim_event_element_1
    inputs:
    - table_name: tenant_dim
      file_name: ./tests/insert_tenant_dim_1.sql
      file_type: sql
    ...
    outputs:
    - table_name: p5_dim_event_element
      file_name: ./tests/validate_p5_dim_event_element_1.sql
      file_type: sql
  ```

1. Support multiple testcase definitions as a test suite. Test suite execution may be automated for non-regression testing to ensure continuous quality.
1. Once tests are completed, tear down tables and data.
  ```sh
  shift_left table delete-unit-tests <table-name>
  ```
1. Do not impact other tables that may be used to do integration tests within the same Kafka Cluster. For that there is a postfix string add to the name of the tables. This postfix is defined in the config.yaml file as:
  ```yaml
  app:
    post_fix_unit_test: _ut
  ```
  This post_fix can be anything, but try to use very short string.

The following diagram illustrates the global infrastructure deployment context:

<figure markdown="span">
![2](./images/test_frwk_infra.drawio.png){ width=750 }
<figcaption>Test Harness environment and scope</figcaption>
</figure>


* One the left, the developer's computer is used to run the test harness tool and send Flink statements to Confluent Cloud environment/ compute pool using the REST API. The Flink API key and secrets are used. 
* The Flink statement under test is the same as the one going to production, except the tool may change the name of the source tables to use the specified postfix. The postfix is defined in the config.yaml file as `app.post_fix_unit_test` parameter.
* The green cylenders represent Kafka Topics which are mapped to Flink source and sink tables. They are defined specifically by the tool.
* As any tables created view Flink on Confluent Cloud have schema defined in schema registry, then schema context is used to avoid conflict within the same cluster. 

The following diagram illustrates the target unit testing environment:

<figure markdown="span">
![2](./images/test_frwk_design.drawio.png){ width=750 }
<figcaption>Deployed Flink statements for unit testing</figcaption>
</figure>


## Unit-test Usage and Recipes

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
  shift_left table init-unit-tests <table_name> --nb-test-cases 2 
  # example with the user_role (using the naming convention)
  shift_left table init-unit-tests int_p3_user_role --nb-test-cases 2 
  ```

  In long run it is recommended to define only one test case.

* For each input table, of the dml under test, there will be ddl and dml script files created with the numbered postfix to match the unit test it supports (e.g _1 in `insert_src_p3_roles_1.sql`) for inserting records to the table `src_p3_roles` in the context of test case # 1.  In the example below the `dml_user_role.sql` has 3 input tables: `src_p3_users`, `src_p3_tenants`, `src_p3_roles`. For each of those input tables, a foundation ddl is created to create the table with "_ut" postfix (or the prefix defined in `app.post_fix_unit_test` in the config.yaml), this is used for test isolation: `ddl_src_p3_roles.sql`, `ddl_src_p3_users.sql` and `ddl_src_p3_tenants.sql`
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

The 2 test cases are created as you can see in the `test_definitions.yaml`
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

The two test cases use different approaches to define the data: SQL and CSV files. This is a more flexible solution, so the tool can inject data, as csv rows. The csv data may come from an extract of kafka topic records.

* Data engineers **update the content** of the insert statements and **the validation statements** to reflect business requirements. Once done, try unit testing with the command:
  ```sh
  shift_left table  run-unit-tests <table_name> --test-case-name test_<table_name>_1 
  ```

A test execution may take some time as it performs the following steps:

1. Read the test definition.
1. Execute the ddl for the input tables with '_ut' postfix to keep specific test data.
1. Insert records in input tables.
1. Create a new output table with the specified postfix.
1. Deploy the DML to test
1. Deploy the validation SQL for each test case.
1. Build test report


* To run the one test :
  ```sh
  # change the table name and test case name
  shift_left table run-unit-tests <table_name> --test-case-name <test_case_1> 
  ```

  This command executes the creation of the DDLs to create the input tables with a postfix like ('_ut') and the insert SQLs to inject synthetic data.

  This execution is re-entrant, which means it will NOT recreate the DDLs if the topics are already present, and skill to the next step. But executing multiple times the insert data will generate duplicates to the input tables. What we do observed is that the most complex SQL script to develop is the validation one, so the command support running the validation as a separate command and multiple times.

* Run your validation script:
  ```sh
  shift_left table run-validation-tests  <table_name> --test-case-name <test_case_1> 
  ```

* Clean the tests artifacts created on Confluent Cloud with the command:
  ```sh
  shift_left table delete-unit-tests <table_name>
  ```

### Running with more data

The second test case created by the `shift_left table init-unit-tests ` command uses a csv file to demonstrate how the tool can manage more data. It is possible to extract data from an existing topics, as csv file. The tool, as of now, is transforming the rows in the csv file as SQL insert values. 

In the future it could direcly write to a Kafka topics that are the input tables for the dml under test.

Data engineers may use the csv format to create a lot of records. Now the challenge will be to define the validation SQL script, but this is another story.

## Integration tests

### Context 

The logic of integration tests is to validate end-to-end processing for a given pipeline and assess the time to process records from sources to facts or sink tables. Integration tests are designed to test end-to-end data flow across multiple tables and pipelines within a project. This is inherently a project-level concern and not a pipelines concern.

The approach is to keep those integration tests at the same level as the `pipelines` folder of the project, but organize the tests by data product. As an example for the data product, `c360`, and the analytical data build from the `fact_users`, the hierarchy will look like:

```sh
pipelines
tests
  └── c360
      └── fact_users
```

The content of the folder will include all the insert statements for the raw topics of the pipeline and the validation SQLs for intermediates and facts. 

The synthetic data are injected at the raw topics with unique identifier and time stamp so it will be easier to develop the validation script and compute the end-to-end latency:

The following figure illustrates those principles:

![](./images/test_frwk_flink_pipeline.drawio.png)

The data to build is for the sink `F`, so integration tests will validate all the purple Flink statements which are ancestors to `F`. The integration tests insert data for the two input raw topics used to build the `src_x` ans `src_y`.

Intermediate validations may be added to assess the state of the intermediate Flink statement output, but the most important one is the SQL validation of the output of `F`. 

The tool supports to create insert SQLs and the last validation script. But there are challenges to address. It was assessed that we could use Kafka header to add to metadata attribute: a unique id and a timestamp. The classical way to do so, is to alter the raw tables with:

```sql
ALTER TABLE raw_groups add headers MAP<STRING, STRING> METADATA;
```

Then each input statement to the raw topic includes a map construct to define the correlation id and the timestamp:

```sql
INSERT INTO raw_users (user_id, user_name, user_email, group_id, tenant_id, created_date, is_active, headers) VALUES
-- User 1: Initial record
('user_001', 'Alice Johnson', 'alice.johnson@example.com', 'admin', 'tenant_id_001', '2023-01-15', true, map('cor_id', 'cor_01', 'timestamp', now()));
```

but this approach means we need to modify all the intermediate Flink statements to pass those metadata to their output table. 

```sql
select
  -- ...
  headers
from final_table;
```

Also at each intermediate statement there will be the following challenges to address:

* On any join, which tx_id to use, or does a concatenation approach being used?
* Which timestamp to use from the two tables joined?
* Finally how to ensure that, at each table, records are created to the output table(s): it is possible that input record may be filtered out, and not output record is created, meaning the latency is becoming infinite.

So the solution is to adapt and use existing fields in the input to set a `cor_id` and a `timestamp`. 

Instead if generating a timestamp. when the raw_topic is the outcome of Debezium CDC, there is a the `ts_ms` field that can be used as timestamp, but it also needs to be propagated down the sink.

### Initialize the integration test 

The command to create a scaffolding:

```sh
shift_left project init-integration-tests F
```

### Running the integration tests:

```sh
shift_left project run-integration-tests F
```

### Tearsdown:

```sh
shift_left project delete-integration-tests F
```

With these capabilities, we can also assess the time to process records from source to sink tables.

### Feature tracking

- [x] init tests folder, with data product and sink table folder
- [ ] For test isolation in shared environment and cluster, the name of the table will have a postfix defined in config.yaml and defaulted with `_it`
- [ ] get all alter table for the raw tables
- [ ] get all the insert synthetic data for raw_table
- [ ] build a validation SQL query to validate the message arrive and compute a delta, insert this to a it_test_topic
- [ ] Support Kafka consumer created for output sink table

## Unit Test Harness Code Explanation

The classes to support unit tests processing are:

![](./images/th_classes.drawio.png){ width=900 }
