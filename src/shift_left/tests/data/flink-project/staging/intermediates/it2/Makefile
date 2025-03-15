include ../../../common.mk

# --- it2

# --- Entry point
create_it2: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, it2)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, ('dev-ddl-it2',), ./sql-scripts/ddl.it2.sql) 
	$(call delete_flink_statement, ('dev-ddl-it2',) )

describe_flink_ddl:
	$(call describe_flink_statement,  ('dev-ddl-it2',))

delete_flink_ddl:
	$(call delete_flink_statement, ('dev-ddl-it2',))

drop_table_it2: 
	$(call drop_table, drop-it2, it2)
	$(call delete_flink_statement, drop-it2)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-dml-it2)

create_flink_dml:
	$(call create_flink_statement, dev-dml-it2, ./sql-scripts/dml.it2.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-dml-it2, ./tests/dml.it2.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-dml-it2)

pause_flink_dml:
	$(call pause_flink_statement dev-dml-it2)

resume_flink_dml:
	$(call resume_flink_statement dev-dml-it2)

delete_schemas:
	$(call delete_schema, it2-value)
	$(call delete_schema, it2-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_it2