include ../../../common.mk

# --- it1

# --- Entry point
create_it1: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, it1)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-ddl-it1, ./sql-scripts/ddl.it1.sql) 
	$(call delete_flink_statement, dev-ddl-it1 )

describe_flink_ddl:
	$(call describe_flink_statement,  dev-ddl-it1)

delete_flink_ddl:
	$(call delete_flink_statement, dev-ddl-it1)

drop_table_it1: 
	$(call drop_table, drop-it1, it1)
	$(call delete_flink_statement, drop-it1)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-dml-it1)

create_flink_dml:
	$(call create_flink_statement, dev-dml-it1, ./sql-scripts/dml.it1.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-dml-it1, ./tests/dml.it1.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-dml-it1)

pause_flink_dml:
	$(call pause_flink_statement dev-dml-it1)

resume_flink_dml:
	$(call resume_flink_statement dev-dml-it1)

delete_schemas:
	$(call delete_schema, it1-value)
	$(call delete_schema, it1-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_it1