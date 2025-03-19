env_name="aws-west"

if [ "$#" -eq 1 ]
then
    env_name= $1
fi

echo $env_name

export ENV_ID=$(confluent environment list | grep $env_name | awk -F '|' '{gsub(/^[ \t]+/,"",$2);gsub(/[ \t]+$/,"",$2); print $2}')
echo $ENV_ID