
from typing import Optional, get_type_hints
from pydantic_avro.base import AvroBase
from datetime import datetime
import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from app_config import read_config

SQLS_PATH="../flink_sqls/tmp"

class record_execution_state_record(AvroBase):
        id: str
        ts_ms: datetime
        tenant_id: Optional[str] = None
        deleted: bool = False
        app_id: str
        object_state_id: Optional[str] = None
        appId: Optional[str] = None
        owner: Optional[str] = None
        title: Optional[str] = None
        dueDate: Optional[str] = None
        duration: Optional[str] = None
        tenantId: Optional[str] = None
        closedDate: Optional[str] = None
        launchDate: Optional[str] = None
        recordname: Optional[str] = None
        recordtype:Optional[str] = None
        description:Optional[str] = None
        durationUnit: Optional[str] = None
        modifiedDate: Optional[str] = None
        recordNumber: Optional[str] = None
        recordStatus: Optional[str] = None
        affectedsites: Optional[str] = None
        completedDate: Optional[str] = None
        parentRecordId: Optional[str] = None
        executionPlanId: Optional[str] = None
        childWorkflowIds: Optional[str] = None
        workflowNodeState: Optional[str] = None
        workflowRevision: Optional[str] = None
        recordConfigurationId: Optional[str] = None
        durationInBusinessDays: int = 1
        excludeRecordsFromGlobalSearch: Optional[str] = None
        excludeRecordsFromMyCollection: Optional[str] = None
        data_type: Optional[str] = None
        series_id: Optional[str] = None
        revision_number: int =1
        source_ts_ms:  Optional[datetime]
        dl_landed_at: Optional[str] = None


def define_schemas(config, klass, name):
  sr_client= SchemaRegistryClient({"url": config["registry"]["url"], 
                                   "basic.auth.user.info":  config["registry"]["registry_key_name"]+":"+config["registry"]["registry_key_secret"]})
  schema = json.dumps(klass.avro_schema())
  sc = Schema(schema_str=schema, schema_type="AVRO")
  schema_id=sr_client.register_schema(subject_name=name, schema=sc)
  print(f"Registered {klass} as {schema_id}")


def upload_schemas_to_registry(config):
  """ Important that the subject name for the schema has the nam of the topic + "-value" so
  it can be automatically associated to the topic
  """
  define_schemas(config, record_execution_state_record,config["app"]["topics"]["record_execution_state_record_topic"] + "-value")
  
def write_avro_file(klass, name):
  schema = json.dumps(klass.avro_schema())
  with open("./avro/" + name + ".avsc", "w") as f:
    f.write(schema)
  f.close()

# map from topic to type
known_model_classes = { "record_execution_state_record_raw": record_execution_state_record}

def generate_avros(config):
  for schema_name in config["app"]["topics"]:
    write_avro_file(known_model_classes[schema_name], config["app"]["topics"][schema_name] +  "-value")

def work_field(name,col_type):
    print(f" work_field {name}  {col_type}")
    if isinstance(col_type, list):
      if col_type[0] == "null":
        name,col_type=work_field(name,"string")
      else:
        name,col_type=work_field(name,col_type[0])
    elif isinstance(col_type, dict):
        name, col_type=work_field(name,col_type["type"])
    elif col_type == "long":
        col_type = "BIGINT"
    else:
       col_type=col_type.upper()
    return name, col_type
       
def generate_raw_sql(klass):
  #print(klass.__fields__)
  sql_str=f"CREATE TABLE {klass.__name__} (\n"
  sql_columns=""
  avro= klass.avro_schema()
  for field in avro["fields"]:
    name,col_type= work_field(field["name"], field["type"])
    sql_columns+=f"   `{name}`  {str(col_type)},\n"
    
  sql_str+=sql_columns[:-2]
  sql_str+="\n) WITH (\n"
  sql_str+="     'changelog.mode' = 'upsert',\n"
  sql_str+="     'scan.bounded.mode' = 'unbounded',\n"
  sql_str+="     'scan.startup.mode' = 'earliest-offset',\n"
  sql_str+="     'kafka.retention.time' = '0'\n"
  sql_str+=" \n);\n"
  return sql_str

if __name__ == "__main__":
  config = read_config()
  generate_avros(config)
  #upload_schemas_to_registry(config)
  for t in config["app"]["topics"]:
    s =config["app"]["topics"][t]
    sql=generate_raw_sql(known_model_classes[s])
    print(sql)