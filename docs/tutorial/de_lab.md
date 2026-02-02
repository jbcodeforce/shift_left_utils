# Lab: Day to Day Data Engineer's Work

This lab presents a simple flow for standard activities Data Engineer may do to develop Confluent Flink Solution.

## Pre-requisite

During this lab, you will work on an existing Flink project, therefore clone the repository:

```sh
git clone https://github.com/jbcodeforce/flink_project_demos.git
cd flink_project_demos
```

* Set your configuration file and environment variables as presented in [the setup lab](./setup_lab.md)
	```sh
	export PIPELINES=$FLINK_PROJECT/pipelines
	export CONFIG_FILE=$FLINK_PROJECT/config.yaml
	```
	
## Create table

## Unit test table

## Assess Pipeline

* Start from a white page: The pipeline_definition.json files are, per table and local to your local folder. It may possible those files were created in git. But it is recommended to clean your local copy:
	```sh
	shift_left pipeline delete-all-metadata
	```

Recall that it will work from $PIPELINES folder.

* Rebuild the metadata with your local work.

## Deploy one to many tables
