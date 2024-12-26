# Shift left Tools and AI utilities

This repository includes a set of tools to help refactoring SQL batch processing to real-time prodessing using Apache Flink. 

[Read the utility documentation in book format](https://jbcodeforce.github.io/shift_left_utils/).

Working on this kind of refactoring projects is taking some time and are challenging. AI Agentic solution should help data engineers to shift their data pipelines to the real-time processing by offering SQL translation tools.

This repository is a tentative to develop and share some of those tools and practices for running such projects.

As of now the utilities are oriented to use Confluent Cloud for Kafka and for Flink, but running local Flink and Kafka should be easy to support.

To avoid calling remote LLM, the current repository use Ollama, running locally, or potentially in a remote server on-premises or inside a private network.


## What is in plan for this repository

* [x] A docker image for the python environment to run the existing python tools
* [x] A docker compose file to run Ollama and the python environment to be able to do dbt refactoring 
* [x] A set of python tools to do code generation
