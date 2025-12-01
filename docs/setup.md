# Environment Setup - Deeper dive

???- info "Versions"
    Created January 2025 - Updated 03/21/25: setup guide separation between user of the CLI and developer of this project.
    Update 08/09: config file explanation and how to get the parameters


[The new setup lab](./tutorial/setup_lab.md) addresses how to setup the `shift_left` cli and how to validate the installation. 

This chapter addresses configuration review and tuning. 

## The config.yaml file

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
      cluster_type: stage
      src_topic_prefix: clone
      reject_topics_prefixes: ["clone","dim_","src_"]
    ```


=== "Confluent cloud"
    
    * Get environment ID from the `Environment details` in the Confluen Console. The cloud provider and region. 

    ```yaml
    confluent_cloud:
      environment_id:  env-20xxxx
      region:  us-west-2
      provider:  aws
      organization_id: 5329.....96
    ```
    * The `organization_id` is defined under the `user account > Organization settings`
   
=== "Flink"
    * Flink settings are per environment. Get the URL endpoint by going to `Environments > one_of_the_env > Flink > Endpoints`, copy the private or public endpoint
    ```yaml
    flink:
        compute_pool_id: lfcp-0...
        catalog_name:  dev-flink-us-west-2
        database_name:  dev-flink-us-west-2
    ```

    * The compute pool id is used as default for running Flink query.
    * Catalog name is the name of the environment and database name is the name of the Kafka cluster

=== "App"
    The app section defines a set of capabilities to tune the cli.
    ```yaml
    app:
        accepted_common_products: ['common', 'seeds']
        sql_content_modifier: shift_left.core.utils.table_worker.ReplaceEnvInSqlContent
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
    


### Configuration File Setup

* Update the content of the config.yaml to reflect your Confluent Cloud environment. (For the commands used for migration, you do not need Kafka settings.)
  ```yaml
  # Confluent Cloud Configuration
  confluent_cloud:
    organization_id: "YOUR_CLUSTER_ID"
    environment_id: "YOUR_ENVIRONMENT_ID"
    region: "YOUR_REGION"
    provider: "aws"
  flink:
    compute_pool_id: "YOUR_COMPUTE_POOL_ID"
    catalog_name: "environment_name"
    database_name: "kafka_cluster_name"
  ```


* Set the following environment variables before using the tool. This can be done by:
    ```sh
    cp src/shift_left/src/shift_left/core/templates/set_env_temp ./set_env
    ```

    Modify the CONFIG_FILE, FLINK_PROJECT, SRC_FOLDER, SL_LLM_* variables

* Source it:
    ```sh
    source set_env
    ```

* Validate config.yaml
	```bash
	shift_left project validate-config
	```

???- warning "Security access"
    The config.yaml file is ignored in Git. So having the keys in this file is not a major concern, as it is used by the developers only. But it may be possible, in the future, to access secrets using a Key manager API. This could be a future enhancement.

## Environment variables

This document explains how to use environment variables to securely manage API keys and secrets instead of storing them in config.yaml files.

The Shift Left utility now supports environment variables for sensitive configuration values. Environment variables take precedence over config.yaml values, allowing you to:

- Keep sensitive data out of configuration files
- Use different credentials for different environments
- Securely manage secrets in CI/CD pipelines
- Follow security best practices

### Kafka Section
| Environment Variable | Config Path | Description |
|---------------------|-------------|-------------|
| `SL_KAFKA_API_KEY` | `kafka.api_key` | Kafka API key |
| `SL_KAFKA_API_SECRET` | `kafka.api_secret` | Kafka API secret |

### Confluent Cloud Section
| Environment Variable | Config Path | Description |
|---------------------|-------------|-------------|
| `SL_CONFLUENT_CLOUD_API_KEY` | `confluent_cloud.api_key` | Confluent Cloud API key |
| `SL_CONFLUENT_CLOUD_API_SECRET` | `confluent_cloud.api_secret` | Confluent Cloud API secret |

### Flink Section
| Environment Variable | Config Path | Description |
|---------------------|-------------|-------------|
| `SL_FLINK_API_KEY` | `flink.api_key` | Flink API key |
| `SL_FLINK_API_SECRET` | `flink.api_secret` | Flink API secret |


### Priority Order

The setting will use the following order:

1. **Environment Variables** (highest priority)
2. **Config.yaml values** (fallback)
3. Default values set in the code

If an environment variable is set, it will override the corresponding value in config.yaml. If a value is set in config.yaml it will be override the default value.

### Setting Environment Variables

#### Bash/Zsh
```bash
export SL_KAFKA_API_KEY="your-kafka-api-key"
export SL_KAFKA_API_SECRET="your-kafka-api-secret"
export SL_FLINK_API_KEY="your-flink-api-key"
export SL_FLINK_API_SECRET="your-flink-api-secret"
export SL_CONFLUENT_CLOUD_API_KEY="your-confluent-cloud-api-key"
export SL_CONFLUENT_CLOUD_API_SECRET="your-confluent-cloud-api-secret"
```

#### Using .env File
Create a `.env` file (don't commit this to version control):
```
SL_KAFKA_API_KEY=your-kafka-api-key
SL_KAFKA_API_SECRET=your-kafka-api-secret
SL_FLINK_API_KEY=your-flink-api-key
SL_FLINK_API_SECRET=your-flink-api-secret
SL_CONFLUENT_CLOUD_API_KEY=your-confluent-cloud-api-key
SL_CONFLUENT_CLOUD_API_SECRET=your-confluent-cloud-api-secret
```

Then load it before running your application:
```bash
set -a && source .env && set +a
```

**Never commit secrets to version control**: Use `.gitignore` to exclude `.env` files

#### Common Issues

1. **"Missing environment variables" error**
   - Check that environment variable names are correct (case-sensitive)
   - Verify that variables are exported in your shell
   - Use `env | grep SL to see what's set

1. To see all supported environment variables, you can call the help function in Python:
```python
from shift_left.core.utils.app_config import print_env_var_help
print_env_var_help()
```


