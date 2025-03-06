include ../../../common.mk

# --- a_table

# --- Entry point
create_a_table: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, a_table)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-ddl-a-table, ./sql-scripts/ddl.a_table.sql) 
	$(call delete_flink_statement, dev-ddl-a-table )

describe_flink_ddl:
	$(call describe_flink_statement,  dev-ddl-a-table)

delete_flink_ddl:
	$(call delete_flink_statement, dev-ddl-a-table)

drop_table_a_table: 
	$(call drop_table, drop-a-table, a_table)
	$(call delete_flink_statement, drop-a-table)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-dml-a-table)

create_flink_dml:
	$(call create_flink_statement, dev-dml-a-table, ./sql-scripts/dml.a_table.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-dml-a-table, ./tests/dml.a_table.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-dml-a-table)

pause_flink_dml:
	$(call pause_flink_statement dev-dml-a-table)

resume_flink_dml:
	$(call resume_flink_statement dev-dml-a-table)

delete_schemas:
	$(call delete_schema, a_table-value)
	$(call delete_schema, a_table-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_a_table