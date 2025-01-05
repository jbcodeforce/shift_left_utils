# Environment Setup

This chapter discusses the essential tools required and the project setup. There are two options for running the tools: one involves using a Python virtual environment, while the other utilizes Docker along with a predefined Python environment image that includes all the necessary modules and code.

Additionally, running the Ollama Qwen model with 32 billion parameters requires 64 GB of memory and a GPU with 32 GB of VRAM. Therefore, it may be more practical to create an EC2 instance with the appropriate resources to handle the migration of each fact and dimension table, generate the staged migrated SQL, and then terminate the instance. Currently, tuning the pipeline and finalizing each SQL version is done manually.

To create this EC2 machine, Terraform configurations are defined in [the IaC folder. See the readme.](https://github.com/jbcodeforce/shift_left_utils/tree/main/IaC/tf_aws_ec2)

## Common Pre-requisites

* On Windows - [enable WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
* All Platforms - [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* All Platforms - [install make for windows](https://gnuwin32.sourceforge.net/packages/make.htm), 

    * Mac OS: ```brew install make``` 
    * Linux: ```sudo apt-get install build-essential```

* All Platforms - [install confluent cli](https://docs.confluent.io/confluent-cli/current/install.html)
* Go to the folder parent to the dbt source project. For example if your dbt project is in /home/user/code then be in this code folder.
* Clone this repository: 

```sh
git clone  https://github.com/jbcodeforce/shift_left_utils.git
```

* Use the `setup.sh` script to create the project structure, and copy some important files, for the new flink project: 

```sh
cd shift_left_utils
./setup.sh <a_flink_project_name | default is flink_project>
```

At this stage, you should have three folders for the project: flink_project, the dbt_source, the shift_left_utils. For the flink_project a pipelines folder with the same structure as defined by the Kimball guidelines:

```sh
├── flink-project
│   ├── docker-compose.yaml
│   ├── pipelines
│   │   ├── common.mk
│   │   ├── dimensions
│   │   ├── facts
│   │   ├── intermediates
│   │   ├── sources
│   │   └── stage
│   └── start-ollama.sh
├── shift_left_utils
└── src_dbt_project
```

* Connect to Confluent Cloud with CLI, then get environment and compute pool identifiers:

```sh
confluent login --save
```

* Get the credentials for the Confluent Cloud Kafka cluster and Flink compute pool. If you do not have such environment Confluent cli has a quickstart plugin:

```sh
confluent flink quickstart --name dbt-migration --max-cfu 50 --region us-west-2 --cloud aws
```


* Modify the `config.yaml` with the corresponding values. 

    The Kafka section is to access the topics

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

    Those declarations are loaded by the Kafka Producer and Consumer and with tools accessing the model definitons from the Schema Registy. (see utils/kafka/app_config.py code)

* Modify the value for the cloud provider, the environment name, and the confluent context in the file `pipelines/common.mk`. The first lines of this common makefile file have some variables set up:

  ```
  ENV_NAME=aws-west    # name of the Confluent Cloud environment
  CLOUD=aws            # cloud provider
  REGION=us-west-2     # cloud provider region where compute pool and kakfa cluster run
  MY_CONTEXT= login-jboyer@confluent.io-https://confluent.cloud 
  DB_NAME=cluster_0.   # Name of the Kafka Cluster, mapped to Database name in Flink
  ```

???- warning "Security access"
  THe key are in the config.yaml file that is ignore in Git. But it can be possible in the future to access secrets using Key manager API. This could be a future enhancement.

## Using Python Virtual Environment

### Pre-requisites

* All Platforms - [Install Python 3.12](https://www.python.org/downloads/release/python-3120/)
* Create a virtual environment:

```sh
python -m venv .venv
```

* Activate the environment:

```sh
source .venv/bin/activate
```

* Work from the shift_left_utils folder
* Install the needed Python modules

```sh
# under the shift_left_utils folder
pip install -r utils/requirements.txt
```

* Define environment variables, change the flink_project folder to reflect your own settings:

```sh
export CONFIG_FILE=../../flink-project/config.yaml
export SRC_FOLDER=../../src-dbt-project/models
export STAGING=../../flink-project/staging
export REPORT_FOLDER=../../flink-project/reports
```

* Install Ollama: [using one of the downloads](https://ollama.com/download).
* Start Ollama using `ollama serve` then download the one of the Qwen model used by the AI Agent: `qwen2.5-coder:32b` or `qwen2.5-coder:14b` depending of your memory and GPU resources.

You are ready to use the different tools, as a next step, read the migration approach in [this note](./migration.md#migration-process)

## Using Docker Pythonenv image

Use this set up if you do not want to use a virtual environment.

### Pre-requisites

* All platforms - [install Docker](https://docs.docker.com/engine/install/)

    * On Linux - complete post-install for docker without sudo
    * With Nvidia GPU - [install NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html#installation)
    * On Windows - [enable Docker in WSL2](https://docs.docker.com/desktop/wsl/#enabling-docker-support-in-wsl-2-distros)

* All Platforms - [install Docker Compose plugin](https://docs.docker.com/compose/install/)

* The docker version used for the test and development of this repository was"

```sh
docker version 
version 27.3.1, build ce12230
```

If for some reason, you could not use Docker Desktop, you can try [Colima](https://github.com/abiosoft/colima/blob/main/README.md), Ranger Desktop, or podman.

For colima the following configuration was used to run those tools on Mac M3 64Gb ram: 

```sh
colima start --cpu 4 -m 48 -d 100 -r docker
```

### Project setup

* Modify the `docker-compose.yaml` file to reference the source code folder path mapped to dbt-src folder. For example, using a source project, named `src-dbt-project`, defined at the same level as the root folder of this repository, the statement looks like:

```yaml
  pythonenv:
    image: jbcodeforce/shift-left-utils
    # ... more config here
    volumes:
    # change the path below
    - ../src-dbt-project/:/app/dbt-src
    # do not touch the other mounted volumes
```

You are ready to use the different tool, as a next step read an example of migrstion approach in [this note](./migration.md#migration-process)



