# Environment Setup

This chapter addresses the basic tool needed and the project setup.

## Pre-requisites

* You need [git cli](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Clone this repository: 
```sh
git clone  https://github.com/jbcodeforce/shift_left_utils.git
```

* Get [docker engine](https://docs.docker.com/engine/install/) with the docker cli.

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

Modify the `docker-compose.yaml` file to reference the source code folder path mapped to dbt-src folder. For example, using a source project, named `dbt-project-src`, defined at the same level as the root folder of this repository, the statement looks like:

```yaml
  pythonenv:
    image: jbcodeforce/shift-left-utils
    # ... more config here
    volumes:
    # change the path below
    - ../dbt-project-src/:/app/dbt-src
    # do not touch the other mounted volumes
```
