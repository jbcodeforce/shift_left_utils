
echo "confluent environment uses $ENV_ID"
 
export CPOOL_ID=$(confluent flink compute-pool list | awk -F '|' '{gsub(/^[ \t]+/,"",$2);gsub(/[ \t]+$/,"",$2); print $2}' | tail -1)
echo $CPOOL_ID