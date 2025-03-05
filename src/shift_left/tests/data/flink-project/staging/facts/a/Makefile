include ../../../common.mk

# --- a

# --- Entry point
create_a: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, a)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-ddl-a, ./sql-scripts/ddl.a.sql) 
	$(call delete_flink_statement, dev-ddl-a )

describe_flink_ddl:
	$(call describe_flink_statement,  dev-ddl-a)

delete_flink_ddl:
	$(call delete_flink_statement, dev-ddl-a)

drop_table_a: 
	$(call drop_table, drop-a, a)
	$(call delete_flink_statement, drop-a)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-dml-a)

create_flink_dml:
	$(call create_flink_statement, dev-dml-a, ./sql-scripts/dml.a.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-dml-a, ./tests/dml.a.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-dml-a)

pause_flink_dml:
	$(call pause_flink_statement dev-dml-a)

resume_flink_dml:
	$(call resume_flink_statement dev-dml-a)

delete_schemas:
	$(call delete_schema, a-value)
	$(call delete_schema, a-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_a