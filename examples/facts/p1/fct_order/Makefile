include ../../../common.mk

# --- fct_order

# --- Entry point
create_fct_order: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, fct_order)

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, dev-p1-ddl-fct-order, ./sql-scripts/ddl.fct_order.sql) 

describe_flink_ddl:
	$(call describe_flink_statement,  dev-p1-ddl-fct-order)

delete_flink_ddl:
	$(call delete_flink_statement, dev-p1-ddl-fct-order)

drop_table_fct_order: 
	$(call drop_table, drop-, fct_order)
	$(call delete_flink_statement, drop-)

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  dev-p1-dml-fct-order)

create_flink_dml:
	$(call create_flink_statement, dev-p1-dml-fct-order, ./sql-scripts/dml.fct_order.sql) 

create_flink_dev:
	$(call create_flink_statement, dev-p1-dml-fct-order, ./tests/dml.fct_order.sql) 

delete_flink_dml:
	$(call delete_flink_statement, dev-p1-dml-fct-order)

pause_flink_dml:
	$(call pause_flink_statement dev-p1-dml-fct-order)

resume_flink_dml:
	$(call resume_flink_statement dev-p1-dml-fct-order)

delete_schemas:
	$(call delete_schema, fct_order-value)
	$(call delete_schema, fct_order-key)

delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas delete_topic_fct_order