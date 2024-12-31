# Environment Setup

This chapter addresses the basic tools needed and the project setup. There are two options to run the tools, one using a python virtual environment, and the other one is to use docker and a pythonenv predefined image with all the necessary module and code.

## Common Pre-requisites

* On Windows - [enable WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
* All Platforms - [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* All Platforms - [install confluent cli](https://docs.confluent.io/confluent-cli/current/install.html)
* Go to the folder parent to the dbt source project. For example if your dbt project is in /home/user/code then be in this code folder.
* Clone this repository: 

  ```sh
  git clone  https://github.com/jbcodeforce/shift_left_utils.git
  ```

* Use setup.sh script to create the project structure for the new flink project: 

  ```sh
  cd shift_left_utils
  ./setup.sh <a_flink_project_name | default is flink_project>
  ```

  At this stage you should have three folders for the project, and for the flink_project a pipelines placeholder

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

* Connect to Confluent Cloud with CLI, then get environment and compute pool

```sh
confluent login --save
```

* Get the credentials for the Confluent Cloud Kafka cluster and Flink compute pool. If you do not have such environment confluent cli has a quickstart plugin:

```sh
confluent flink quickstart --name dbt-migration --max-cfu 50 --region us-west-2 --cloud aws
```


* Modify the config.yaml with the corresponding values
* Modify the value for the cloud provider, the environment name, and the confluent context in the file `pipelines/common.mk`

## Using Python Virtual Environment

### Pre-requisites

* All Platforms - [Install Python 3.12 or 3.13]()
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

* Define environment variables

```sh
export CONFIG_FILE=../../flink-project/config.yaml
export SRC_FOLDER=../../src-dbt-project/models
export STAGING=../../flink-project/staging
export REPORT_FOLDER=../../flink-project/reports
```

* Install Ollama: [using one of the downloads](https://ollama.com/download).
* Start Ollama using `ollama serve` then download the one of the Qwen model used by the AI Agent: `qwen2.5-coder:32b` or `qwen2.5. coder:14b` depending of your memory and GPU.

Yor are ready to use the different tool, as a next step read an example of migrstion approach in [this note](./migration.md#migration-process)

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

* Copy the `docker-compose.yaml` file from the shift-left-project as you need to reference the source project, and the config.yaml template:

  ```sh
  cp shift_left_utils/docker-compose.yaml flink-project
  cp shift_left_utils/utils/config_tmpl.yaml flink-project/config.yaml
  ```

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



