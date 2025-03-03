# Environment Setup

This chapter discusses the essential tools required and the project setup. There are two options for running the tools: one involves using a Python virtual environment, while the other utilizes Docker along with a predefined Python environment image that includes all the necessary modules and code.

Additionally, running the Ollama Qwen model with 32 billion parameters requires 64 GB of memory and a GPU with 32 GB of VRAM. Therefore, it may be more practical to create an EC2 instance with the appropriate resources to handle the migration of each fact and dimension table, generate the staged migrated SQL, and then terminate the instance. Currently, tuning the pipeline and finalizing each SQL version is done manually.

To create this EC2 machine, Terraform configurations are defined in [the IaC folder. See the readme.](https://github.com/jbcodeforce/shift_left_utils/tree/main/IaC/tf_aws_ec2) With Terraform and setup.sh script the next sections are automated. The EC2 does not need to run docker.

## Common Pre-requisites

* On Windows - [enable WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
* All Platforms - [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* All Platforms - [install make for windows](https://gnuwin32.sourceforge.net/packages/make.htm), 

    * Mac OS: ```brew install make``` 
    * Linux: ```sudo apt-get install build-essential```

* All Platforms - [install confluent cli](https://docs.confluent.io/confluent-cli/current/install.html)
* All Platforms - [Install Python 3.12](https://www.python.org/downloads/release/python-3120/)

* Create a virtual environment:

```sh
python -m venv .venv
```

* Activate the environment:

```sh
source .venv/bin/activate
```

* Clone this repository: 

```sh
git clone  https://github.com/jbcodeforce/shift_left_utils.git
```

* Install the shift_left CLI using the command (this is temporary once the CLI will be loaded to pypi): 

```sh
pip install src/shift_left/dist/shift_left-0.1.0-py3-none-any.whl
```

* Validate the CLI is available via:

```sh
shift_left --help
```

???- info "Rebuild the CLI"
      The way the CLI is built, is by using [uv](https://docs.astral.sh/uv) and the command:
      `uv build` 

## Create a new Flink data project

This is step is only valuable when starting a new project, or a new data product using Flink. 

```sh
shift_left project init <project_name> <project_path> --project-type 
# example for a default Kimball project
shift_left project init flink-project ../
# For a project more focused on developing data product
shift_left project init flink-project ../ --project-type data-product
```

At this stage, you should have three folders for the project: flink_project, the dbt_source, the shift_left_utils. For the flink_project a pipelines folder with the same structure as defined by the Kimball guidelines:

```sh
├── flink-project
│   ├── pipelines
│      ├── common.mk
│      ├── dimensions
│      ├── facts
│      ├── intermediates
│      ├── sources
│      └── stage
└── src_dbt_project
```

or for a data product:

```sh
├── pipelines
│   ├── common.mk
│   └── data_product_1
│       ├── dimensions
│       ├── facts
│       │   └── fct_order
│       │       ├── Makefile
│       │       ├── sql-scripts
│       │       │   ├── ddl.fct_order.sql
│       │       │   └── dml.fct_order.sql
│       │       ├── tests
│       │       └── tracking.md
│       ├── intermediates
│       └── sources
└── staging
```

## Working in a project

* Start a Terminal
* Connect to Confluent Cloud with CLI, then get the environment and compute pool identifiers:

```sh
confluent login --save
```

* Get the credentials for the Confluent Cloud Kafka cluster and Flink compute pool. If you do not have such environment Confluent cli has a quickstart plugin:

```sh
confluent flink quickstart --name dbt-migration --max-cfu 50 --region us-west-2 --cloud aws
```

* Define environment variables in the .env file

```sh
FLINK_PROJECT=.
CCLOUD_ENV_NAME=
CLOUD_PROVIDER=
CLOUD_REGION=
CCLOUD_CONTEXT=
CCLOUD_KAFKA_CLUSTER=
CCLOUD_COMPUTE_POOL_ID=
SRC_FOLDER=../../src-dbt-project/models
STAGING=$FLINK_PROJECT/staging
PIPELINES=$FLINK_PROJECT/pipelines
```

* Modify the `config.yaml` in the root of the Flink project, with the corresponding values. The Kafka section is to access the Kafka Cluster and topics:

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



You are ready to use the different tools, as a next step read an example of migration approach in [this note](./migration.md#migration-process) or use the [recipes](./recipes.md) to get how to do some common activities.


## Working with the migration AI agent

* Install Ollama: [using one of the downloads](https://ollama.com/download).
* Start Ollama using `ollama serve` then download the one of the Qwen model used by the AI Agent: `qwen2.5-coder:32b` or `qwen2.5-coder:14b` depending of your memory and GPU resources.



