include ../../../common.mk

# --- src_users_groups
TABLE_NAME=src_users_groups
SCRIPT_PRE_KEBAB=j9r
SCRIPT_POST_KEBAB=$(subst _,-,$(TABLE_NAME))

DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-ddl-$(SCRIPT_POST_KEBAB)
DROP_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-drop-$(SCRIPT_POST_KEBAB)
DDL_SCRIPT=./sql-scripts/ddl.$(TABLE_NAME).sql
DML_STATEMENT=$(SCRIPT_PRE_KEBAB)-dml-$(SCRIPT_POST_KEBAB)
DML_SCRIPT=./sql-scripts/dml.$(TABLE_NAME).sql
DML_TESTS_SCRIPT=./tests/dml.$(TABLE_NAME).sql

# --- raw_groups (test data)
RAW_TABLE_NAME=raw_groups
TO_DDL_NAME=raw-groups
RAW_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-ddl-$(TO_DDL_NAME)
RAW_DROP_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-drop-$(TO_DDL_NAME)
RAW_DDL_SCRIPT=./tests/ddl.$(RAW_TABLE_NAME).sql
INSERT_GROUPS_STATEMENT=$(SCRIPT_PRE_KEBAB)-insert-groups
INSERT_GROUPS_SCRIPT=./tests/dml.insert_groups.sql

# --- Entry point
create_src_users_groups: create_flink_ddl  create_flink_dml

# --- Entry point for test data setup
setup_test_data: create_raw_groups_ddl insert_test_groups


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

delete_schemas:
	$(call delete_schema, $(TABLE_NAME)-value)
	$(call delete_schema, $(TABLE_NAME)-key)


# --- Raw Groups DDL (for test data)
create_raw_groups_ddl:
	$(call create_flink_statement, $(RAW_DDL_STATEMENT), $(RAW_DDL_SCRIPT))
	$(call delete_flink_statement, $(RAW_DDL_STATEMENT))

describe_raw_groups_ddl:
	$(call describe_flink_statement, $(RAW_DDL_STATEMENT))

delete_raw_groups_ddl:
	$(call delete_flink_statement, $(RAW_DDL_STATEMENT))

drop_raw_groups_table:
	$(call drop_table, $(RAW_DROP_DDL_STATEMENT), $(RAW_TABLE_NAME))
	$(call delete_flink_statement, $(RAW_DROP_DDL_STATEMENT))

# --- Insert Test Groups Data
insert_test_groups:
	$(call create_flink_statement, $(INSERT_GROUPS_STATEMENT), $(INSERT_GROUPS_SCRIPT))
	$(call delete_flink_statement, $(INSERT_GROUPS_STATEMENT))

describe_insert_groups:
	$(call describe_flink_statement, $(INSERT_GROUPS_STATEMENT))

delete_insert_groups:
	$(call delete_flink_statement, $(INSERT_GROUPS_STATEMENT))

delete_flink_statements: delete_flink_ddl delete_flink_dml delete_raw_groups_ddl delete_insert_groups

delete_data: delete_schemas