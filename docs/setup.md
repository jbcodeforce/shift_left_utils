# Environment Setup

## Pre-requisites

* You need [git cli](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Clone this repository: `git clone  https://github.com/jbcodeforce/shift_left_utils.git`
* Get [docker engine](https://docs.docker.com/engine/install/) and docker cli.

```sh
# docker version used
version 27.3.1, build ce12230
```

If for some reason, you could not use Docker Desktop, you can try [Colima](https://github.com/abiosoft/colima/blob/main/README.md), Ranger Desktop, or podman.

For colima the following configuration was used: `colima start -c 4 -m 32 -d 100 -r docker`

## Project setup

Modify the docker-compose to reference where the source code folder is.