include ../../../common.mk

# --- src_common_tenant
TABLE_NAME=src_common_tenant
SCRIPT_PRE_KEBAB=j9r
SCRIPT_POST_KEBAB=$(subst _,-,$(TABLE_NAME))

DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-ddl-$(SCRIPT_POST_KEBAB)
DROP_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-drop-$(SCRIPT_POST_KEBAB)
DDL_SCRIPT=./sql-scripts/ddl.$(TABLE_NAME).sql
DML_STATEMENT=$(SCRIPT_PRE_KEBAB)-dml-$(SCRIPT_POST_KEBAB)
DML_SCRIPT=./sql-scripts/dml.$(TABLE_NAME).sql
RAW_DDL_SCRIPT=./tests/ddl.raw_tenants.sql
RAW_DATA_SCRIPT=./tests/insert_raw_data.sql
RAW_DATA_STATEMENT=$(SCRIPT_PRE_KEBAB)-raw-data

# --- Entry point
create_src_common_tenant: create_flink_ddl  create_flink_dml


# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, $(TABLE_NAME))

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, $(DDL_STATEMENT), $(DDL_SCRIPT)) 
	$(call delete_flink_statement, $(DDL_STATEMENT) )


describe_flink_ddl:
	$(call describe_flink_statement,  $(DDL_STATEMENT))

delete_flink_ddl:
	$(call delete_flink_statement, $(DDL_STATEMENT))

drop_table: 
	$(call drop_table, $(DROP_DDL_STATEMENT), $(TABLE_NAME))
	$(call delete_flink_statement, $(DROP_DDL_STATEMENT))

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  $(DML_STATEMENT))

create_flink_dml:
	$(call create_flink_statement, $(DML_STATEMENT), $(DML_SCRIPT)) 

create_flink_dev:
	$(call create_flink_statement, $(DML_STATEMENT), $(DML_TESTS_SCRIPT)) 

delete_flink_dml:
	$(call delete_flink_statement, $(DML_STATEMENT))

pause_flink_dml:
	$(call pause_flink_statement $(DML_STATEMENT))

resume_flink_dml:
	$(call resume_flink_statement $(DML_STATEMENT))

delete_flink_statements: delete_flink_ddl delete_flink_dml

create_raw_data:
	$(call create_flink_statement, 'raw-ddl', $(RAW_DDL_SCRIPT)) 
	$(call create_flink_statement, $(RAW_DATA_STATEMENT), $(RAW_DATA_SCRIPT)) 

delete_flk_statement_raw_data:
	$(call delete_flink_statement, 'raw-ddl')
	$(call delete_flink_statement, $(RAW_DATA_STATEMENT))

delete_raw_data:
	$(call drop_table, dev_drop_raw, raw_tenants)
	$(call delete_flink_statement, dev_drop_raw)


