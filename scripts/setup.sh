project_name="flink-project"

if [ "$#" -eq 1 ]
then
    project_name=$1
fi


mkdir ../$project_name
mkdir ../$project_name/pipelines
cp ./utils/templates/common.mk ../$project_name/pipelines/common.mk
cp ./utils/config_tmpl.yaml ../$project_name/config.yaml
