# Environment Setup

???- info "Version"
    Created January 2025 - Updated 03/21/25: setup guide separation between user of the CLI and developer of this project.

This chapter discusses the CLI tool to manage Flink project, in a shift_left exercise or in close future in a data as a product project. To install the CLI, which is based on Python, use a virtual environment.

Additionally, when using the tool to do migration of existing code to Apache Flink SQL, a LLM running within Ollama is used. The Qwen model with 32 billion parameters requires 64 GB of memory and a GPU with 32 GB of VRAM. Therefore, it may be more practical to create an EC2 instance with the appropriate resources to handle the migration of each fact and dimension tables, generate the stageg migrated SQLs, and then terminate the instance. Currently, tuning the pipeline and finalizing each automatically migrated SQL is done manually as some migrations are not perfect. There is work to be done in prompting and chaining AI agents.

To create this EC2 machine, Terraform configurations are defined in [the IaC folder. See the readme.](https://github.com/jbcodeforce/shift_left_utils/tree/main/IaC/tf_aws_ec2) With Terraform and setup.sh script the next sections are automated. The EC2 does not need to run docker.

**Important** Ollama and LLM is used for migration, and to classify SQL statements.

## Pre-requisites

* On Windows - [enable WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
* All Platforms - [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* All Platforms - [install make for windows](https://gnuwin32.sourceforge.net/packages/make.htm), 

    * Mac OS: ```brew install make``` 
    * Linux: ```sudo apt-get install build-essential```

* All Platforms - [install confluent cli](https://docs.confluent.io/confluent-cli/current/install.html)
* All Platforms - [Install Python 3.12](https://www.python.org/downloads/release/python-3120/)

* Clone this repository (this will not be necessary once the CLI will be available in pypi): 

```sh
git clone  https://github.com/jbcodeforce/shift_left_utils.git
cd shift_left_utils
```

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

* Install the shift_left CLI using the command (this is temporary once the CLI will be loaded to pypi): To get the list of version of the wheel, thery are under the `src/shift_left/dist` folder, the last version as of 4/11 is 0.1.8 but it will change, so the documentation may not reflect the last version.

```sh
pip install src/shift_left/dist/shift_left-0.1.18-py3-none-any.whl
```


### Specific install for the developers using uv

The installation is simpler with 

```sh
uv tool list
uv tool uninstall shift_left
uv tool install shift_left@src/shift_left/dist/shift_left-0.1.18-py3-none-any.whl
```


### Set up configuration yaml file

The configuration file has to be named config.yaml and will be referenced by the environment variables: CONFIG_FILE.

* Copy the  `config.yaml` template file to keep some important parameters for the CLI. 

```sh
cp src/shift_left/src/shift_left/core/templates/config_tmpl.yaml ./config.yaml
```

* Modify the `config.yaml`, with the corresponding values. Some sections are mandatory

The Kafka section is to access the Kafka Cluster and topics :

```yaml
kafka:
  bootstrap.servers: pkc-<uid>.us-west-2.aws.confluent.cloud:9092
  security.protocol: SASL_SSL
  sasl.mechanisms: PLAIN
  sasl.username: <key name>
  sasl.password: <key seceret> 
  session.timeout.ms: 5000
```

The registry section is for the schema registy.

```yaml
registry:
  url: https://psrc-<uid>.us-west-2.aws.confluent.cloud
  registry_key_name: <registry-key-name>
  registry_key_secret: <registry-key-secrets>
``` 

Those declarations are loaded by the Kafka Producer and Consumer and with tools accessing the model definitions from the Schema Registy.


???- warning "Security access"
    The config.yaml file is ignored in Git. So having the keys in this file is not a major concern as it used by the developer only. But it can be possible, in the future, to access secrets using a Key manager API. This could be a future enhancement.

### Environment variables


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
export CCLOUD_COMPUTE_POOL_ID=lfcp-xxxx
```

To get the environment variables configured for your Terminal session do:

```sh
source set_env
```

### Validate the CLI

```sh
shift_left --help
```

#### Next>> [Recipes section](recipes.md)

