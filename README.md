# Shift left Tools and AI utilities

This repository includes a set of tools to help refactoring SQL batch processing to real-time prodessing using Apache Flink. 

[Read the utility documentation in book format](https://jbcodeforce.github.io/shift_left_utils/).


## What is in plan for this repository

* [x] A docker image for the python environment to run the existing python tools
* [x] A docker compose file to run Ollama and the python environment to be able to do dbt refactoring 
* [x] A set of python tools to do code generation
* [x] Add verification if table already exist in the migrated tables
* [x] Generate sink folder structure with makefile
* [x] Capability of re-generating Makefile. 
* [x] Makefile supporting drop table and dev specific dml
* [x] Improve different prompts with one shot prompting: -> one or two shots prompting gave worse results.
* [ ] Verify concat(...) generated statement becomes md5(concat(...))
* [ ] Test Harness framework: - define sql for insert test data to all tables used for a given flink statement, define the sql to validate the outcome. 
* [ ] Deployment of a flink statement with control of children deployment based on the semantic of the flink statement.
