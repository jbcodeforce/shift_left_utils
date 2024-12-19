# Shift left Tools and AI utilities

Shift Left means taking bach-processing job and try to refactor them to real-time processing. In batch processing a lot of projects use SQL and dbt (Data build tool). In real-time processing, Apache Kafka is a de-facto middleware to persist immutable records, and for SQL, Python and Java based real-time processing Apache Flink is also the preferred platform.

Working on this kind of refactoring projects is taking some time and are challenging. AI Agentic solution should help.

This repository is a tentative to develop and share some tools and practices for running such project.

As of now the utilities are oriented to use Confluent Cloud for Kafka and Flink, but running local Flink and Kafka is supported.

## What is in plan for this repository

* [ ] A docker image for the python environment to run the existing python tools
* [ ] A set of python tools to do code generation
* [ ] A docker compose file to run Ollama and the python environment to run AI Agents to help on the refactoring

## Set up

* You need [git cli](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Clone this repository: `git clone  https://github.com/jbcodeforce/shift_left_utils.git`
* Get [docker engine]( https://github.com/jbcodeforce/shift_left_utils.git) and docker cli.

    ```sh
    # docker version used
    version 27.3.1, build ce12230
    ```

* Start docker compose:

    ```sh
    docker compose up -d
    ```


## Kafka tools

### Running the Kafka consumer

```sh
python kafka_avro_consumer.py -t topic_name
```