from datetime import datetime
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any

class MetadataResult(BaseModel):
    self_ref:  Optional[str] =  Field(alias="self", default=None)
    next: Optional[str]

class OpRow(BaseModel):
    op: Optional[int] =  Field(default=None, description="the row number")
    row: List[Any]

class Data(BaseModel):
    data: Optional[List[OpRow]] = []

class StatementResult(BaseModel):
    api_version:  Optional[str] =  Field(default=None,description="The api version")
    kind: Optional[str] =  Field(default=None,description="The StatementResult or nothing")
    metadata: Optional[MetadataResult] =  Field(default=None,description="Metadata for the StatementRsult when present")
    results: Optional[Data]=  Field(default=None, description=" results with data as array of content")


class Metadata(BaseModel):
    created_at: datetime = Field(..., description="Timestamp when the resource was created")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels associated with the resource")
    resource_version: str = Field(..., description="Resource version identifier")
    self: str = Field(..., description="Self URL of the resource")
    uid: str = Field(..., description="Unique identifier for the resource")
    updated_at: datetime = Field(..., description="Timestamp when the resource was last updated")

class Type(BaseModel):
    length: Optional[int] = Field(None, description="Length of the type if applicable")
    nullable: bool
    type: str

class Column(BaseModel):
    name: str
    type: Optional[Type] = Field(None, description="type of the column if applicable")

class Schema(BaseModel):
    columns: Optional[List[Column]] = Field(None, description="columns of the schema definition")

class Traits(BaseModel):
    is_append_only: bool
    is_bounded: bool
    flink_schema: Optional[Schema] =  Field(alias="schema", default=None)
    sql_kind: str
    upsert_columns: Optional[List[Any]] = Field(default=None, description="Upsert columns if applicable")

class Status(BaseModel):
    detail: str
    network_kind: str
    phase: str
    traits: Optional[Traits] = Field(default=None, description="Traits  if applicable")

class Spec(BaseModel):
    compute_pool_id: str
    principal: str
    properties: Optional[Dict] = Field(default=None, description="Additional properties for the statement")
    statement: str
    stopped: bool

class Statement(BaseModel):
    api_version: str
    environment_id: str
    kind: str
    metadata: Metadata
    name: str
    organization_id: str
    spec: Spec
    status: Status
    result: Optional[StatementResult] = Field(default=None, description="Result of the statement execution, for example for a select from...")
    execution_time: Optional[float] = 0
    loop_counter: Optional[int] = 0



if __name__ == '__main__':
    statement = """
 {
      "api_version": "sql/v1",
      "environment_id": "env-nknqp3",
      "kind": "Statement",
      "metadata": {
        "created_at": "2025-03-18T23:40:56.636265Z",
        "labels": {},
        "resource_version": "6",
        "self": "https://flink.us-west-2.aws.confluent.cloud/sql/v1/organizations/49cee212-6346-438a-a1fa-d1b1cbd90d44/environments/env-nknqp3/statements/table-api-2025-03-18-174056-dad6cd4c-8210-4318-b489-042feb2e7130-plugin",
        "uid": "ebd33cc3-0d46-42e1-8cb2-f18e8c136d48",
        "updated_at": "2025-03-18T23:40:57.005289Z"
      },
      "name": "table-api-2025-03-18-174056-dad6cd4c-8210-4318-b489-042feb2e7130-plugin",
      "organization_id": "49cee212-6346-438a-a1fa-d1b1cbd90d44",
      "spec": {
        "compute_pool_id": "lfcp-xvrvmz",
        "principal": "u-xg2ndz",
        "properties": null,
        "statement": "SELECT `CATALOG_ID`, `CATALOG_NAME` FROM `env-nknqp3`.`INFORMATION_SCHEMA`.`CATALOGS`",
        "stopped": false
      },
      "status": {
        "detail": "",
        "network_kind": "PUBLIC",
        "phase": "COMPLETED",
        "traits": {
          "is_append_only": true,
          "is_bounded": true,
          "schema": {
            "columns": [
              {
                "name": "CATALOG_ID",
                "type": {
                  "length": 2147483647,
                  "nullable": false,
                  "type": "VARCHAR"
                }
              },
              {
                "name": "CATALOG_NAME",
                "type": {
                  "length": 2147483647,
                  "nullable": false,
                  "type": "VARCHAR"
                }
              }
            ]
          },
          "sql_kind": "SELECT",
          "upsert_columns": null
        }
      }
    }
"""
    obj= Statement.model_validate_json(statement)
    print(obj.status)