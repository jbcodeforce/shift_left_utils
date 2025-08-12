# Environment Setup

???- info "Versions"
    Created January 2025 - Updated 03/21/25: setup guide separation between user of the CLI and developer of this project.
    Update 08/09: config file explanation and how to get the parameters

This chapter addresses how to set up the shift_left CLI tool to manage Flink project. To install the CLI, which is based on Python, use a virtual environment: venv and pip will work, but we have adopted [uv](https://docs.astral.sh/uv/) for package and virtual environment management.

Additionally, when using the tool to do migration of existing code to Apache Flink SQL, a Large Language Model is needed [see dedicated chapter for instruction](./coding/llm_based_translation.md).

## Pre-requisites

* On Windows - [enable WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
* All Platforms - [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Make is used to encapsulate the confluent cli, to make it easier for Data engineers to deploy Flink statements. 
    * [install make for windows](https://gnuwin32.sourceforge.net/packages/make.htm)
    * Mac OS: ```brew install make``` 
    * Linux: ```sudo apt-get install build-essential```

* All Platforms - [install confluent cli](https://docs.confluent.io/confluent-cli/current/install.html)
* If using uv, install it [using the documentation](https://docs.astral.sh/uv/getting-started/installation/)
* All Platforms - [Install Python 3.12](https://www.python.org/downloads/release/python-3120/)

* Clone this repository (this will not be necessary once the CLI will be available in pypi): 
  ```sh
  git clone  https://github.com/jbcodeforce/shift_left_utils.git
  cd shift_left_utils
  ```

=== "Using Python and pip"

    * Create a Python virtual environment:
        ```sh
        python -m venv .venv
        ```
    * Activate the environment:
        ```sh
        source .venv/bin/activate
        ```
    * Be sure to use the pip install in the virtual environment:
        ```sh
        python -m pip --version
        # if pip not present
        python -m ensurepip
        # install requirements
        python -m pip install -r requirements.txt
        ```
    * Install the shift_left CLI using the command (this is temporary once the CLI will be loaded to pypi): To get the list of version of the wheel, they are under the `src/shift_left/dist` folder
        ```sh
        pip install src/shift_left/dist/shift_left-0.1.30-py3-none-any.whl
        ```

=== "uv"

    * Create a new virtual environment in any folder, but could be the : `uv venv`
    * Activate the environment:
        ```sh
        source .venv/bin/activate
        ```
    * Install the cli:
        ```sh
        uv tool list
        # in case it was already installed
        uv uninstall shift_left
        uv install shift_left src/shift_left/dist/shift_left-0.1.30-py3-none-any.whl
        ```

## Set up config.yaml file

The configuration file `config.yaml` is used intensively to tune the shift_left per environment and will be referenced by the environment variables: CONFIG_FILE. You should have different config.yaml for the different kafka cluster, schema registry and Flink environment.

* Copy the `config_tmpl.yaml` template file to keep some important parameters for the CLI. 
  ```sh
  cp src/shift_left/src/shift_left/core/templates/config_tmpl.yaml ./config.yaml
  ```

* Modify the `config.yaml` with values from your Confluent Cloud settings. See the tabs below for the different sections of this file: 

=== "Kafka section"
    * Get the Kakfa cluster URL using the `Confluent Console > Cluster Settings` page. The URL has the cluster id and a second id that is used for the RESP API.

    ```yaml
    kafka:
      bootstrap.servers: lkc-2qxxxx-pyyyy.us-west-2.aws.confluent.cloud:9092
      cluster_id: lkc-2qxxxx
      pkafka_cluster: lkc-2qxxxx-pyyyy
      security.protocol: SASL_SSL
      sasl.mechanisms: PLAIN
      sasl.username: <key name>
      sasl.password: <key seceret> 
      session.timeout.ms: 5000
      cluster_type: stage
      src_topic_prefix: clone
    ```
    * the sasl.username is the api key name, and the sasl.password is the api secrets. Those are created in the `Kafka cluster > API Keys`. The key can be downloaded locally

=== "Confluent cloud"
    
    * Get environment ID from the `Environment details` in the Confluen Console. The cloud provider and region. 

    ```yaml
    confluent_cloud:
      base_api: api.confluent.cloud/org/v2
      environment_id:  env-20xxxx
      region:  us-west-2
      provider:  aws
      organization_id: 5329.....96
      api_key: T3.....HH
      api_secret: secret-key
      page_size: 100
      glb_name: glb
      url_scope: private
    ```
    * The `organization_id` is defined under the `user account > Organization settings`
    * the url_scope is set to `private` or `public`. When using private link it should be private. 
    * When using a global load balancer, the glb_name to be set. It will be use to define the URL of the REST API.
    * The API key and secrets are defined per user, and visible  under the `user account > API keys`

=== "Flink"
    * Flink settings are per environment. Get the URL endpoint by going to `Environments > one_of_the_env > Flink > Endpoints`, copy the private or public endpoint
    ```yaml
    flink:
        flink_url: flink-d....7.us-west-2.aws.glb.confluent.cloud
        api_key: MVXI.....HY   
        api_secret: cf.......IztA
        compute_pool_id: lfcp-0...
        catalog_name:  dev-flink-us-west-2
        database_name:  dev-flink-us-west-2
        max_cfu: 50       #-- maximum CFU when creating compute pool via the tool
        max_cfu_percent_before_allocation: .8   #-- percent * 100 to consider before creating a new compute pool 
    ```

    * Define the API keys from `Environments > one_of_the_env > Flink > API keys`.
    * The compute pool id is used as default for running Flink query.
    * Catalog name is the name of the environment and database name is the name of the Kafka cluster

=== "App"
    The app section defines a set of capabilities to tune the cli.
    ```yaml
    app:
        delta_max_time_in_min: 15  # this is to apply randomly to the event timestamp to create out of order
        report_output_dir: ./reports
        default_PK: __db
        timezone: America/Los_Angeles
        src_table_name_prefix: src_
        logging: INFO
        products: ["p1", "p2", "p3"]
        accepted_common_products: ['common', 'seeds']
        cache_ttl: 120 # in seconds
        sql_content_modifier: shift_left.core.utils.table_worker.ReplaceEnvInSqlContent
        translator_to_flink_sql_agent: shift_left.core.utils.translator_to_flink_sql.DbtTranslatorToFlinkSqlAgent
        dml_naming_convention_modifier: shift_left.core.utils.naming_convention.DmlNameModifier
        compute_pool_naming_convention_modifier: shift_left.core.utils.naming_convention.ComputePoolNameModifier
        data_limit_where_condition : rf"where tenant_id in ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = {product_name})"
        data_limit_replace_from_reg_ex: r"\s*select\s+\*\s+from\s+final\s*;?"
        data_limit_table_type: source
        data_limit_column_name_to_select_from: tenant_id
        post_fix_unit_test: _ut
        post_fix_integration_test: _it
    ```

    *   `post_fix_unit_test, post_fix_integration_test` are used to append the given string to table name during unit testing and integration test respectively.
    * The `data_limit_replace_from_reg_ex, data_limit_table_type, data_limit_column_name_to_select_from` are used to add data filtering to all the source tables based on one column name to filter. The regex specifies to file the `select * from final` which is the last string in most Flink statements using CTEs implementation. 
    * `sql_content_modifier` specifies the custom class to use to do some SQL content modification depending of the target environment. This is a way to extend the CLI logic to specific usage.
    

???- warning "Security access"
    The config.yaml file is ignored in Git. So having the keys in this file is not a major concern, as it is used by the developers only. But it may be possible, in the future, to access secrets using a Key manager API. This could be a future enhancement.

## Environment variables


Ensure the following environment variables are set: in a `set_env` file. For example, in a project where the source repository is cloned to your-src-dbt-folder and the target Flink project is flink-project, use these setting:

```sh
export FLINK_PROJECT=$HOME/Code/flink-project
export STAGING=$FLINK_PROJECT/staging
export PIPELINES=$FLINK_PROJECT/pipelines
export SRC_FOLDER=$HOME/Code/datawarehouse/models
export CONFIG_FILE=./config.yaml
# The following variables are used when deploying with the Makefile.

export CCLOUD_ENV_ID=env-xxxx
export CCLOUD_ENV_NAME=j9r-env
export CCLOUD_KAFKA_CLUSTER=jxxxxx
export CLOUD_REGION=us-west-2
export CLOUD_PROVIDER=aws
export CCLOUD_CONTEXT=login-<user_id>@<domain>.com-https://confluent.cloud
```

To get the environment variables configured for your Terminal session do:

```sh
source set_env
```

## Validate the CLI

```sh
shift_left version
shift_left --help
shift_left project list-topics $PIPELINES
```

#### Next>> [Recipes section](recipes.md)

