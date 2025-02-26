include ../../../common.mk

# --- int_table_2

# --- Entry point
create_int_table_2: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, int_table_2)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-p1-ddl-int-table-2, ./sql-scripts/ddl.int_table_2.sql) 
	$(call delete_flink_statement, dev-p1-ddl-int-table-2 )

describe_flink_ddl:
	$(call describe_flink_statement,  dev-p1-ddl-int-table-2)

delete_flink_ddl:
	$(call delete_flink_statement, dev-p1-ddl-int-table-2)

drop_table_int_table_2: 
	$(call drop_table, drop-int_table_2, int_table_2)
	$(call delete_flink_statement, drop-int_table_2)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-p1-dml-int-table-2)

create_flink_dml:
	$(call create_flink_statement, dev-p1-dml-int-table-2, ./sql-scripts/dml.int_table_2.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-p1-dml-int-table-2, ./tests/dml.int_table_2.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-p1-dml-int-table-2)

pause_flink_dml:
	$(call pause_flink_statement dev-p1-dml-int-table-2)

resume_flink_dml:
	$(call resume_flink_statement dev-p1-dml-int-table-2)

delete_schemas:
	$(call delete_schema, int_table_2-value)
	$(call delete_schema, int_table_2-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_int_table_2