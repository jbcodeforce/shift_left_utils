
echo "confluent environment uses $ENV_ID"
 
export CLUSTER_ID=$(confluent kafka cluster list --environment $ENV_ID | awk -F '|' '{gsub(/^[ \t]+/,"",$2);gsub(/[ \t]+$/,"",$2); print $2}' | tail -1)
echo $CLUSTER_ID