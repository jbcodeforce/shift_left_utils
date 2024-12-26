# Shift Left tools and practices

## Introduction

Shift Left means taking bach-processing jobs and try to refactor them to real-time processing using a product such as Apache Flink. In batch processing a lot of projects use SQL and dbt (Data build tool) to define the logic of the data pipeline. In real-time processing, Apache Kafka is a de-facto middleware to persist immutable records, and for SQL, Python and Java based real-time processing, Apache Flink is also the preferred platform.



* Start docker compose, the first time it may take some time.

```sh
docker compose up -d
```

## Command Summary

## A migration path