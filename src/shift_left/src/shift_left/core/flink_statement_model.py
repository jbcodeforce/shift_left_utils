"""
Copyright 2024-2025 Confluent, Inc.
"""
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
    metadata: Optional[MetadataResult] =  Field(default=None,description="Metadata for the StatementResult when present")
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
    detail: Optional[str] =  Field(default=None)
    network_kind: Optional[str] =  Field(default=None)
    phase: Optional[str] =  Field(default=None)
    traits: Optional[Traits] = Field(default=None, description="Traits  if applicable")

class Spec(BaseModel):
    compute_pool_id: str
    principal: str
    properties: Optional[Dict] = Field(default=None, description="Additional properties for the statement")
    statement: str
    stopped: bool

class Statement(BaseModel):
    api_version: Optional[str] =  Field(default=None)
    environment_id: Optional[str] =  Field(default=None)
    kind: Optional[str] =  Field(default=None)
    metadata: Optional[Metadata]= Field(default=None)
    name: str
    organization_id: Optional[str] =  Field(default=None)
    spec: Optional[Spec] = Field(default=None)
    status: Optional[Status] = Field(default= None)
    result: Optional[StatementResult] = Field(default=None, description="Result of the statement execution, for example for a select from...")
    execution_time: Optional[float] = Field(default=0)
    loop_counter: Optional[int] = Field(default=0)

class StatementInfo(BaseModel):
    """
    Keep the needed information for redeploying statement
    """
    name:   Optional[str] =  Field(default=None, description="Unique name of the Flink statement")
    status_phase:   Optional[str] =  Field(default=None, description="Current state of the Flink Statement")
    status_detail:   Optional[str] =  Field(default=None, description="Current state detail of the Flink Statement")
    sql_content:  Optional[str] =  Field(default=None, description="Current sql content of the Flink Statement")
    compute_pool_id:   Optional[str] =  Field(default=None, description="Compute pool id hosting the statement")
    principal:   Optional[str] =  Field(default=None, description="Principal service account")
    sql_catalog:  Optional[str] =  Field(default=None, description="Flink catalog name")
    sql_database:  Optional[str] =  Field(default=None, description="Flink database name")