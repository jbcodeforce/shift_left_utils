# Improve pipeline quality with AI

* Use AI to help managing flow with intent: goal is to bring ingestion and transformation to anyone. Chat with the Confluent Flink assistant to create pipeline, filters for error, route to DLQ, build aggregates, prepare data from cdc and sink connector to S3 or to Database or with Tableflow. 
* Should help for beginner to build PoC or build prototype fast
* Look at sql content to do grouping if there is a 1 to 1 dependency
* Assess deployed statements and propose improvement, help Data engineer to manage and monitor Flink statements.
* AI can help tracking data and lineage and create Atlas metadata files
* Looking at records to process, and flink statement logic it can decide to use one compute pool per statement or group
* Propose solution when statement has logic related to compute field values from date, and be reassessed every day
* Look at data content to understand it

