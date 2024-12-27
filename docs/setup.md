# Environment Setup

This chapter addresses the basic tool needed and the project setup.

## Pre-requisites

* On Windows - [enable WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
* All Platforms - [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* All platforms - [install Docker](https://docs.docker.com/engine/install/)

  * On Linux - complete post-install for docker without sudo
  * With Nvidia GPU - [install NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html#installation)
  * On Windows - [enable Docker in WSL2](https://docs.docker.com/desktop/wsl/#enabling-docker-support-in-wsl-2-distros)

* All Platforms - [install Docker Compose plugin](https://docs.docker.com/compose/install/)
* All Platforms - [install confluent cli](https://docs.confluent.io/confluent-cli/current/install.html)

* Clone this repository: 
```sh
git clone  https://github.com/jbcodeforce/shift_left_utils.git
```

* The docker version used for the test and development of this repository was"

```sh
docker version 
version 27.3.1, build ce12230
```

If for some reason, you could not use Docker Desktop, you can try [Colima](https://github.com/abiosoft/colima/blob/main/README.md), Ranger Desktop, or podman.

For colima the following configuration was used to run those tools on Mac M3 64Gb ram: 

```sh
colima start --cpu 4 -m 32 -d 100 -r docker
```


## Project setup

* Create a folder for your new project that will keep the Flink SQL statements for the different pipeline: As an example we use `flink-project` at the same level as the cloned repository.

```sh
mkdir my-project
tree
├── src-dbt-project
├── flink-project
├── shift_left_utils
    ├── Dockerfile
│   ├── README.md
│   ├── docker-compose.yaml
```

* Copy the `docker-compose.yaml` file from the shift-left-project as you need to reference the source project, and the config.yaml template:

  ```sh
  cp shift_left_utils/docker-compose.yaml flink-project
  cp shift_left_utils/utils/config_tmpl.yaml flink-project/config.yaml
  ```

* Modify the `docker-compose.yaml` file to reference the source code folder path mapped to dbt-src folder. For example, using a source project, named `dbt-project-src`, defined at the same level as the root folder of this repository, the statement looks like:

```yaml
  pythonenv:
    image: jbcodeforce/shift-left-utils
    # ... more config here
    volumes:
    # change the path below
    - ../src-dbt-project/:/app/dbt-src
    # do not touch the other mounted volumes
```

* Get the credentials for the Confluent Cloud Kafka cluster and Flink compute pool. If you do not have such environment confluent cli has a quickstart plugin:

```sh
confluent flink quickstart --name dbt-migration --max-cfu 50 --region us-west-2 --cloud aws
```

  