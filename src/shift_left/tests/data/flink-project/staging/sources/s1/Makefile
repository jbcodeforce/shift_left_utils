include ../../../common.mk

# --- src_s1

# --- Entry point
create_src_s1: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, src_s1)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-ddl-src-s1, ./sql-scripts/ddl.src_s1.sql) 
	$(call delete_flink_statement, dev-ddl-src-s1 )

describe_flink_ddl:
	$(call describe_flink_statement,  dev-ddl-src-s1)

delete_flink_ddl:
	$(call delete_flink_statement, dev-ddl-src-s1)

drop_table_src_s1: 
	$(call drop_table, drop-src-s1, src_s1)
	$(call delete_flink_statement, drop-src-s1)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-dml-src-s1)

create_flink_dml:
	$(call create_flink_statement, dev-dml-src-s1, ./sql-scripts/dml.src_s1.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-dml-src-s1, ./tests/dml.src_s1.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-dml-src-s1)

pause_flink_dml:
	$(call pause_flink_statement dev-dml-src-s1)

resume_flink_dml:
	$(call resume_flink_statement dev-dml-src-s1)

delete_schemas:
	$(call delete_schema, src_s1-value)
	$(call delete_schema, src_s1-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_src_s1