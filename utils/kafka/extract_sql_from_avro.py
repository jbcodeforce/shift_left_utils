import requests
import json
from app_config import read_config
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

"""
Build a SQL statment to create table from a schema subject in Confluent registry
"""
VERSION="1"
SQLS_PATH="../flink_sqls/tmp"


def map_schema_to_sql(subject: str, version: str) -> str:
    URL=f"http://localhost:8081/subjects/{subject}-value/versions/{version}"
    sr_client= SchemaRegistryClient({"url": config["registry"]["url"], 
                                   "basic.auth.user.info":  config["registry"]["registry_key_name"]+":"+config["registry"]["registry_key_secret"]})
  
    response = requests.get(URL)
    print(response.json())
    json_schema= response.json()["schema"]
    fields = json.loads(json_schema)["fields"]
    sql_str=f"CREATE TABLE {subject} (\n"
    sql_attributes=""
    key_name="id"
    for idx,f in enumerate(fields):
        name= f["name"]
        if idx == 0:
            key_name=name
        if type(f["type"]) != dict:
            col_type=f["type"].upper()
        else:
            # 
            print(type)
            col_type="TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'"
        sql_attributes+="   "+ name + " " + col_type + ",\n"
    sql_str+=sql_attributes[:-2]
    sql_str+="\n) WITH (\n"
    sql_str+="'changelog.mode' = 'upsert',\n"
    sql_str+="'scan.bounded.mode' = 'unbounded',\n"
    sql_str+="'scan.startup.mode' = 'earliest-offset',\n"
    sql_str+="'kafka.retention.time' = '0',\n"
    sql_str+=" \n);\n"
    return sql_str

def extract_save_to_file(subject: str, version: str): 
    sql_statment=map_schema_to_sql(subject,version)
    print(sql_statment)
    with open(SQLS_PATH + "/" + subject+".sql","w") as f:
        f.write(sql_statment)
        f.close()


if __name__ == "__main__":
    config=read_config()
    for topic in config["app"]["topics"]:
        print(topic)
        extract_save_to_file(topic + "-value","1")
