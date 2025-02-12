# Those settings may need to be changed
ENV_NAME=aws-west
CLOUD=aws
REGION=us-west-2
MY_CONTEXT= ${CCLOUD_CONTEXT}
DB_NAME=${CCLOUD_KAFKA_CLUSTER}
CPOOLID=${CPOOL_ID}	

# --- common functions
delete_schema = \
	@echo "Attempting to delete schema for subject: $1"; \
	V=$$(confluent schema-registry subject describe $1 2>/dev/null | grep -o '[0-9]*' | head -n 1);\
	echo $$V; \
	if [ -n "$$V" ]; \
	then \
		echo "$1  found"; \
		confluent schema-registry schema delete --subject $1 --version $$V --force; \
		confluent schema-registry schema delete --subject $1 --version $$V --permanent --force; \
	else \
		echo "$1 not found"; \
	fi

create_flink_statement = \
	ENV_ID=$$(confluent environment list | grep ${ENV_NAME} | awk -F '[|\t]' '{ print $$2}' | tr -d ' '); \
	sql_statement=$$(cat $2);\
	confluent flink statement create $1 --sql "$$sql_statement" --database $(DB_NAME) --compute-pool $(CPOOLID)  --environment $$ENV_ID --context $(MY_CONTEXT) --wait 

show_create_table = \
	ENV_ID=$$(confluent environment list | grep ${ENV_NAME} | awk -F '[|\t]' '{ print $$2}' | tr -d ' '); \
	sql_statement="show create table $2";\
	confluent flink statement create $1 --sql "$$sql_statement" --database $(DB_NAME) --compute-pool $(CPOOLID)   --environment $$ENV_ID --context $(MY_CONTEXT) --wait 

drop_table = \
	ENV_ID=$$(confluent environment list | grep ${ENV_NAME} | awk -F '[|\t]' '{ print $$2}' | tr -d ' '); \
	sql_statement="drop table $2";\
	confluent flink statement create $1 --sql "$$sql_statement" --database $(DB_NAME) --compute-pool $(CPOOLID)   --environment $$ENV_ID --context $(MY_CONTEXT) --wait 

describe_flink_statement = \
	confluent flink statement describe $1 --cloud $(CLOUD) --region $(REGION) 
	
pause_flink_statement = \
	confluent flink statement stop $1 --cloud $(CLOUD) --region $(REGION) 

resume_flink_statement = \
	confluent flink statement resume $1 --cloud $(CLOUD) --region $(REGION) 

delete_flink_statement = \
	confluent flink statement delete $1 --cloud $(CLOUD) --region $(REGION) --context $(MY_CONTEXT)

list_topic_content = \
	ENV_ID=$$(confluent environment list | grep ${ENV_NAME} | awk -F '[|\t]' '{ print $$2}' | tr -d ' '); \
	CLUSTER_ID=$$(confluent kafka cluster list --environment $$ENV_ID | awk -F '|' '{sub(/^[ \t]+/,"",$$2);sub(/[ \t]+$$/,"",$$2); print $$2}' | tail -1); \
	confluent kafka topic consume $1 --cluster $$CLUSTER_ID --environment $$ENV_ID --from-beginning

# --- common
list_topics:
	ENV_ID=$$(confluent environment list | grep ${ENV_NAME} | awk -F '[|\t]' '{ print $$2}' | tr -d ' '); \
	CLUSTER_ID=$$(confluent kafka cluster list --environment $$ENV_ID | awk -F '|' '{sub(/^[ \t]+/,"",$$2);sub(/[ \t]+$$/,"",$$2); print $$2}' | tail -1);\
	confluent kafka topic list --cluster  $$CLUSTER_ID --environment $$ENV_ID

# -- some useful targets
flink_list_statements:
	confluent flink statement list --context $(MY_CONTEXT) --output human --cloud $(CLOUD) --region $(REGION)   --compute-pool $$CPOOL_ID

start_flink_shell:
	confluent flink shell --compute-pool $$CPOOL_ID  --context $(MY_CONTEXT) 

# -- init to get environments and compute pool
init: 
	@echo "Set env variables from $(ENV_NAME)"; \
	export CLUSTER_ID=$$(confluent kafka cluster list --environment $$ENV_ID | awk -F '|' '{sub(/^[ \t]+/,"",$$2);sub(/[ \t]+$$/,"",$$2); print $$2}' | tail -1); \
	echo $$ENV_ID $$CLUSTER_ID $$CPOOLID