include ../../../common.mk

# --- table_1

# --- Entry point
create_table_1: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, table_1)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-ddl-table-1, ./sql-scripts/ddl.table_1.sql) 
	$(call delete_flink_statement, dev-ddl-table-1 )

describe_flink_ddl:
	$(call describe_flink_statement,  dev-ddl-table-1)

delete_flink_ddl:
	$(call delete_flink_statement, dev-ddl-table-1)

drop_table_table_1: 
	$(call drop_table, drop-table_1, table_1)
	$(call delete_flink_statement, drop-table_1)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-dml-table-1)

create_flink_dml:
	$(call create_flink_statement, dev-dml-table-1, ./sql-scripts/dml.table_1.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-dml-table-1, ./tests/dml.table_1.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-dml-table-1)

pause_flink_dml:
	$(call pause_flink_statement dev-dml-table-1)

resume_flink_dml:
	$(call resume_flink_statement dev-dml-table-1)

delete_schemas:
	$(call delete_schema, table_1-value)
	$(call delete_schema, table_1-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_table_1