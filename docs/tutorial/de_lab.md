# Lab: Day to Day Data Engineer's Work

This lab presents a simple flow for standard activities Data Engineer may do to develop Confluent Flink Solution.

## Prerequisites

During this lab, you will work on an existing Flink project, therefore clone the repository:

```sh
git clone https://github.com/jbcodeforce/flink_project_demos.git
cd flink_project_demos
```

* Set your configuration file and environment variables as presented in [the setup lab](./setup_lab.md)
	```sh
	export PIPELINES=$FLINK_PROJECT/pipelines
	export CONFIG_FILE=$FLINK_PROJECT/config.yaml
	```

* Be sure to have cloned the flink project demos git repository:
	```sh
	git clone https://github.com/jbcodeforce/flink_project_demos.git
	```

## Create table

During the life of the shift left project, data engineers create need table. The tool support creating table with a common structure. As an example we will add a shipment source table:

```sh
shift_left table init src_shipments $PIPELINES/sources --product-name abc
```

The create folder will look like:

```sh
├── sources
│   └── abc
│       ├── src_shipments
│       │   ├── Makefile
│       │   ├── pipeline_definition.json
│       │   ├── sql-scripts
│       │   │   ├── ddl.src_abc_shipments.sql
│       │   │   └── dml.src_abc_shipments.sql
│       │   ├── tests
│       │   └── tracking.md
```



## Unit test table

* Unit tests may be added to facts, views and dimension tables.The tool looks at the SQL content, the join, from and build test data for each sources.  Here is an example of unit tests added.

```sh
shift_left table init-unit-tests c360_fct_customer_profile  --nb-test-cases 1
```

The new added files are:

```sh
├── c360
│   └── fct_customer_360_profile
│       ├── Makefile
│       ├── pipeline_definition.json
│       ├── sql-scripts
│       │   ├── ddl.c360_fct_customer_profile.sql
│       │   └── dml.c360_fct_customer_profile.sql
│       ├── tests
│       │   ├── ddl_dim_c360_customer_transactions.sql
│       │   ├── ddl_src_c360_app_usage.sql
│       │   ├── ddl_src_c360_customers.sql
│       │   ├── ddl_src_c360_loyalty_program.sql
│       │   ├── ddl_src_c360_support_ticket.sql
│       │   ├── insert_dim_c360_customer_transactions_1.sql
│       │   ├── insert_src_c360_app_usage_1.sql
│       │   ├── insert_src_c360_customers_1.sql
│       │   ├── insert_src_c360_loyalty_program_1.sql
│       │   ├── insert_src_c360_support_ticket_1.sql
│       │   ├── README.md
│       │   ├── test_definitions.yaml
│       │   └── validate_c360_fct_customer_profile_1.sql
```

* If you have a local LLM server it is possible to define environment variables and run the same command with the ai extension so the synthetic data will have coherent data to support joins
	```sh
	# Osaurus on mac environment variables:
	export SL_LLM_BASE_URL=http://localhost:1337/v1
	export SL_LLM_MODEL=qwen3-coder-30b-a3b-instruct-mlx-4bit

	shift_left table init-unit-tests c360_fct_customer_profile  --nb-test-cases 1 --ai
	```

* Tune the data manually to make the test relevant.
* Work on the Validation SQL  to be sure, it reports test failure or success.
* Run the unit test
	```sh
	shift_left table run-unit-tests c360_fct_customer_profile
	```

## Assess Pipeline

* Start from a white page: The pipeline_definition.json files are, per table and local to your local folder. It may possible those files were created in git. But it is recommended to clean your local copy:
	```sh
	shift_left pipeline delete-all-metadata
	```

Recall that it will work from $PIPELINES folder.

* Rebuild the metadata with your local work.
	```sh
	shift_left pipeline build-all-metadata
	```

* Verify an execution plan of one of your table. For example for the view of the customer 360 profile we can do:
	```sh
	shift_left pipeline build-execution-plan --table-name customer_analytics_c360
	```

	The result looks somethig like, with the statement name, using naming convention, the current status of the statement, the compute pool on which the statement may run or is running, and the action the tool will take.

	```sh
	--- Ancestors: 9 ---
	Statement Name                                                  Status          Compute Pool    Action  Upgrade Mode    Table Name
	--------------------------------------------------------------------------------------------------------------------------------------------------------
	---
	dev-usw2-c360-dml-src-c360-app-usage                            STOPPED         lfcp-xvrvmz     To run  Stateful        src_c360_app_usage
	dev-usw2-c360-dml-src-c360-support-ticket                       COMPLET         lfcp-xvrvmz     Skip    Stateful        src_c360_support_ticket
	dev-usw2-c360-dml-src-c360-loyalty-program                      COMPLET         lfcp-xvrvmz     Skip    Stateless       src_c360_loyalty_program
	dev-usw2-c360-dml-src-c360-customers                            COMPLET         lfcp-279od2     Skip    Stateful        src_c360_customers
	dev-usw2-c360-dml-src-c360-transactions                         STOPPED         lfcp-xvrvmz     To run  Stateless       src_c360_transactions
	dev-usw2-c360-dml-src-c360-tx-items                             STOPPED         lfcp-xvrvmz     To run  Stateful        src_c360_tx_items
	dev-usw2-c360-dml-src-c360-products                             COMPLET         lfcp-xvrvmz     Skip    Stateless       src_c360_products
	dev-usw2-c360-dml-dim-c360-customer-transactions                STOPPED         lfcp-x23ggx     To run  Stateful        dim_c360_customer_transactions
	dev-usw2-c360-dml-c360-fct-customer-profile                     STOPPED         lfcp-xvrvmz     To run  Stateful        c360_fct_customer_profile

	--- Children to restart ---
	Statement Name                                                  Status          Compute Pool    Action  Upgrade Mode    Table Name
	--------------------------------------------------------------------------------------------------------------------------------------------------------
	---
	dev-usw2-c360-dml-customer-analytics-c360                       STOPPED         lfcp-xvrvmz     Restart Stateless       customer_analytics_c360
	```

	It is followed to a list of compute pools with their current capacity.

	```sh
	Pool ID         Pool Name                                       Current/Max CFU Flink Statement name
	--------------------------------------------------------------------------------------------------------------------------------------------
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-src-c360-app-usage
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-src-c360-support-ticket
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-src-c360-loyalty-program
	lfcp-279od2     dev-src-c360-customers                          0/30            dev-usw2-c360-dml-src-c360-customers
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-src-c360-transactions
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-src-c360-tx-items
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-src-c360-products
	lfcp-x23ggx     dev-dim-c360-customer-transactions              0/30            dev-usw2-c360-dml-dim-c360-customer-transactions
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-c360-fct-customer-profile
	lfcp-xvrvmz     dev-j9r-pool                                    0/50            dev-usw2-c360-dml-customer-analytics-c360
	```

## Deploy one to many tables

to deploy according to the execution plan, the deployment may include a default compute pool id, if a statement has no current pool id assigned to.

```sh
shift_left pipeline deploy --table-name customer_analytics_c360 --compute-pool-id lfcp-xvrvmz
```
