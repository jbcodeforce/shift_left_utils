# Lab: Project Management

This lab focuses on starting a Confluent Flink project using best practices.

At the highest level the SDLC for flink project may include the following activities, as presented in this Lab:

<figure markdown='span'>
![](./images/flink_dev_flow.drawio.png)
</figure>


##  Prerequisite

You have followed [the setup lab](setup_lab.md) to get shift_left CLI configured and running.


## Initialize a Project

* Go to where you want to create the Flink project
* Execute the project init command:
	```sh
	shift_left project init <project_name> <project_path>
	```

* example for a default Kimball project
	```sh
	shift_left project init dsp .
	```

* For a project more focused on developing data as a product
	```sh
	shift_left project init dsp ../  --project-type data-product
	```

The folder structure looks like:
```sh
dsp
├── docs
├── IaC

├── pipelines
│   ├── common.mk
│   ├── dimensions
│   ├── facts
│   ├── intermediates
│   ├── sources
│   └── views
└── staging
```

## Leveraging Infrastructure As Code

The approach is to use Terraform to create Confluent Cloud Environmen,  Kafka Cluster, Schema Registry and Compute poo
## Add a source table

The source table goal is to remove duplication and filter records, and maybe do transformation from the raw topic. If the raw topic is coming from a CDC, the record structure matches the table structure in the SQL database. Some column may be VARCHAR with a json object inside. It may be relevant to extract those information as new column in a Flink table. Each source table is published to its own raw topic, so there will be one flink statement per raw topic:

<figure markdown='span'>
![](./images/raw_to_src.drawio.png)
</figure>

The goal is to develop a data product for a customer 360 profiling.

* As a Data Engineer the environment variables should be set to point to the created project
	```sh
	export FLINK_PROJECT=$HOME/Code/dsp
	export PIPELINES=$FLINK_PROJECT/pipelines
	export STAGING=$FLINK_PROJECT/staging
	export CONFIG_FILE=...a..path..to..the.config.yaml
	```
* The command to add a table in the context of `c360` data product.
	```sh
	shift_left table init src_customers $PIPELINES/sources --product-name c360
	```

* Now the folder tree looks like this now:
```sh
├── pipelines
│   ├── common.mk
│   ├── dimensions
│   ├── facts
│   ├── intermediates
│   ├── sources
│   │   └── c360
│   │       └── src_customers
│   │           ├── Makefile
│   │           ├── sql-scripts
│   │           │   ├── ddl.src_c360_customers.sql
│   │           │   ├── dml.src_c360_customers.properties
│   │           │   └── dml.src_c360_customers.sql
│   │           ├── tests
│   │           └── tracking.md
```

*The approach is to separate the table creation from the insertion logic.*


### Update the DDL content

Change the DDL content in the file: `ddl.src_c360_c360_customers.sql` to the following content:
	```sql
	CREATE TABLE IF NOT EXISTS src_c360_customers (
		customer_id STRING,
		first_name STRING,
		last_name STRING,
		email STRING,
		phone STRING,
		date_of_birth DATE,
		gender STRING,
		registration_date TIMESTAMP(3),
		customer_segment STRING,
		preferred_channel STRING,
		address_line1 STRING,
		city STRING,
		state STRING,
		zip_code STRING,
		country STRING,
		age_years BIGINT,
		days_since_registration BIGINT,
		generation_segment STRING,
		missing_email_flag BIGINT,
		missing_phone_flag BIGINT,
	PRIMARY KEY(customer_id) NOT ENFORCED
	) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
	WITH (
	'changelog.mode' = 'upsert',
	'key.avro-registry.schema-context' = '.flink-dev',
	'value.avro-registry.schema-context' = '.flink-dev',
	'key.format' = 'avro-registry',
	'value.format' = 'avro-registry',
	'kafka.retention.time' = '0',
	'kafka.producer.compression.type' = 'snappy',
	'scan.bounded.mode' = 'unbounded',
	'scan.startup.mode' = 'earliest-offset',
	'value.fields-include' = 'all'
	);
	```

	The table is upsert changelog mode with a primary key, so records will be deduplicated. As multiple Kafka Clusters are defined in Confluent Cloud environment, and there is one schema registry in t he environement, this is good practices to isolate the schema-context in the schema registry. [See product schema-context documentation](https://docs.confluent.io/platform/current/schema-registry/schema-contexts-cp.html).


### Update the DML content
Do the same for the `dml.src_c360_c360_customers.sql`:
```sql
INSERT INTO src_c360_customers
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    customer_segment,
    preferred_channel,
    address_line1,
    city,
    state,
    zip_code,
    country,
    TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts) age_years,
    TIMESTAMPDIFF(DAY, CAST(registration_date AS TIMESTAMP_LTZ(3)), event_ts) as days_since_registration,
     CASE
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts)  < 25 THEN 'Gen Z'
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts)  < 40 THEN 'Millennial'
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts) < 55 THEN 'Gen X'
        ELSE 'Boomer+' END AS generation_segment,
    CASE
        WHEN email IS NULL
        OR email = '' THEN 1
        ELSE 0 END AS missing_email_flag,
     CASE
        WHEN phone IS NULL
        OR phone = '' THEN 1
        ELSE 0 END AS missing_phone_flag
FROM customers_raw
```
