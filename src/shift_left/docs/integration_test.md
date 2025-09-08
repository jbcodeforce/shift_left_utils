# Integration test harness

## Overview

The integration test harness is a new feature designed to support the definition of end to end statement testing. The synthetic data are injected at the raw topics with unique identifier and time stamp in the header, to be processed by n Flink statements down to the sink table.

Goal is to get the end to end pipeline latency metrics. 

## Features

- Be able to identify a target sink table and add a SQL to track time to get the resulting message in Flink or in a Kafka consumer
- From the sink, get the list of source raw tables, based for synthetic data ingestion
-  
