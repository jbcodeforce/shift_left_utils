include ../../../common.mk

# --- src_users_users
TABLE_NAME=src_c360_users
SCRIPT_PRE_KEBAB=j9r
SCRIPT_POST_KEBAB=$(subst _,-,$(TABLE_NAME))

DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-ddl-$(SCRIPT_POST_KEBAB)
DROP_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-drop-$(SCRIPT_POST_KEBAB)
DDL_SCRIPT=./sql-scripts/ddl.$(TABLE_NAME).sql
DML_STATEMENT=$(SCRIPT_PRE_KEBAB)-dml-$(SCRIPT_POST_KEBAB)
DML_SCRIPT=./sql-scripts/dml.$(TABLE_NAME).sql
DML_TESTS_SCRIPT=./tests/dml.$(TABLE_NAME).sql

# --- raw_users (test data)
RAW_TABLE_NAME=raw_users
TO_DDL_NAME=raw-users
RAW_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-ddl-$(TO_DDL_NAME)
RAW_DROP_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-drop-$(TO_DDL_NAME)
RAW_DDL_SCRIPT=./tests/ddl.$(RAW_TABLE_NAME).sql
INSERT_USERS_STATEMENT=$(SCRIPT_PRE_KEBAB)-insert-users
INSERT_USERS_SCRIPT=./tests/dml.insert_users.sql

# --- Entry point
create_src_users_users: create_flink_ddl  create_flink_dml

# --- Entry point for test data setup
setup_test_data: create_raw_users_ddl insert_test_users


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


# --- Raw Users DDL (for test data)
create_raw_users_ddl:
	$(call create_flink_statement, $(RAW_DDL_STATEMENT), $(RAW_DDL_SCRIPT))
	$(call delete_flink_statement, $(RAW_DDL_STATEMENT))

describe_raw_users_ddl:
	$(call describe_flink_statement, $(RAW_DDL_STATEMENT))

delete_raw_users_ddl:
	$(call delete_flink_statement, $(RAW_DDL_STATEMENT))

drop_raw_users_table:
	$(call drop_table, $(RAW_DROP_DDL_STATEMENT), $(RAW_TABLE_NAME))
	$(call delete_flink_statement, $(RAW_DROP_DDL_STATEMENT))

create_raw_data: create_raw_users_ddl insert_test_data

# --- Insert Test Users Data
insert_test_data:
	$(call create_flink_statement, $(INSERT_USERS_STATEMENT), $(INSERT_USERS_SCRIPT))
	$(call delete_flink_statement, $(INSERT_USERS_STATEMENT))

describe_insert_test_data:
	$(call describe_flink_statement, $(INSERT_USERS_STATEMENT))

delete_insert_test_data:
	$(call delete_flink_statement, $(INSERT_USERS_STATEMENT))

delete_flink_statements: delete_flink_ddl delete_flink_dml delete_raw_users_ddl delete_insert_test_data

delete_data: drop_raw_users_table
