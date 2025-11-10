"""
Copyright 2024-2025 Confluent, Inc.
"""
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any, Set

class MetadataResult(BaseModel):
    self_ref:  Optional[str] =  Field(alias="self", default=None)
    next: Optional[str] = None

class OpRow(BaseModel):
    op: Optional[int] =  Field(default=None, description="the operation type: 0: insert, 1: -U, 2: +U, 3: delete")
    row: List[Any]

class Data(BaseModel):
    data: Optional[List[OpRow]] = []

class StatementResult(BaseModel):
    api_version:  Optional[str] =  Field(default=None,description="The api version")
    kind: Optional[str] =  Field(default=None,description="The StatementResult or nothing")
    metadata: Optional[MetadataResult] =  Field(default=None,description="Metadata for the StatementResult when present")
    results: Optional[Data]=  Field(default=None, description=" results with data as array of content")


class Metadata(BaseModel):
    created_at: str = Field(default=None, description="Timestamp when the resource was created")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels associated with the resource")
    resource_version: str = Field(default=None, description="Resource version identifier")
    self: str = Field(default=None, description="Self URL of the resource")
    uid: str = Field(default=None,  description="Unique identifier for the resource")
    updated_at: str = Field(default=None, description="Timestamp when the resource was last updated")

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

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Statement):
            return NotImplemented
        return self.name == other.name

class ErrorData(BaseModel):
    id: str = Field(default=None)
    status: str = Field(default=None)
    detail: str = Field(default=None)

class StatementError(BaseModel):
    errors: List[ErrorData] = Field(default=[])

class StatementInfo(BaseModel):
    """
    Keep the needed information for redeploying statement
    """
    name:   Optional[str] =  Field(default=None, description="Unique name of the Flink statement")
    status_phase:   Optional[str] =  Field(default=None, description="Current state of the Flink Statement")
    status_detail:   Optional[str] =  Field(default=None, description="Current state detail of the Flink Statement")
    sql_content:  Optional[str] =  Field(default=None, description="Current sql content of the Flink Statement")
    compute_pool_id:   Optional[str] =  Field(default=None, description="Compute pool id hosting the statement")
    compute_pool_name:   Optional[str] =  Field(default=None, description="Compute pool name hosting the statement")
    created_at:   Optional[datetime] =  Field(default=datetime.now(), description="Statement creation date")
    principal:   Optional[str] =  Field(default=None, description="Principal service account")
    sql_catalog:  Optional[str] =  Field(default=None, description="Flink catalog name")
    sql_database:  Optional[str] =  Field(default=None, description="Flink database name")

class StatementListCache(BaseModel):
    created_at: Optional[datetime] = Field(default=datetime.now())
    statement_list: Optional[dict[str, StatementInfo]] = Field(default={})


class FlinkStatementComplexity(BaseModel):
    """
    Keep metrics of the DML statement, derive a complexity of a Flink Statement.

    """
    number_of_regular_joins: int = Field(default=0, description="Number of regular joins in the statement")
    number_of_left_joins: int = Field(default=0, description="Number of left joins in the statement")
    number_of_right_joins: int = Field(default=0, description="Number of right joins in the statement")
    number_of_inner_joins: int = Field(default=0, description="Number of inner joins in the statement")
    number_of_outer_joins: int = Field(default=0, description="Number of outer joins in the statement")
    complexity_type: str = Field(default="Simple", description="Type of complexity")
    state_form: Optional[str] =  Field(default="Stateless", description="Type of Flink SQL statement. Could be Stateful or Stateless")


class FlinkStatementNode(BaseModel):
    """
    To build an execution plan we need one node for each popential Flink Statement to run.
    A node has 0 to many parents and 0 to many children
    """
    # -- static information
    table_name: str
    product_name: str = Field(default="", description="Data Product name")
    type: Optional[str] = Field(default="", description="Type of the node")
    path:  Optional[str] =  Field(default="", description="Name of path to access table files like sql, and metadata")
    created_at: Optional[datetime] = Field(default=datetime.now())
    dml_ref: Optional[str] =  Field(default="", description="DML sql file path")
    dml_statement_name: Optional[str] =  Field(default="", description="DML Statement name")
    ddl_ref: Optional[str] =  Field(default=None, description="DDL sql file path")
    ddl_statement_name: Optional[str] =  Field(default=None, description="DDL Statement name")
    upgrade_mode: str = Field(default="Stateful", description="upgrade mode will depend if the node state is stateful or not.")
    # -- dynamic information
    dml_only: Optional[bool] = Field(default=False, description="Used during deployment to enforce DDL and DML deployment or DML only")
    update_children: Optional[bool] = Field(default=False, description="Update children when the table is not a sink table. Will take care of statefulness. Used during deployment")
    compute_pool_id:  str =  Field(default="", description="Name of compute pool to use for deployment")
    compute_pool_name: Optional[str] =  Field(default="", description="Name of compute pool to use for deployment")
    parents: Set =  Field(default=set(), description="List of parent to run before this node")
    children: Set = Field(default=set(), description="Child list to run after this node")
    existing_statement_info:  Optional[StatementInfo] =  Field(default=None, description="Flink statement status")
    to_run: bool = Field(default=False, description="statement must be executed")
    to_restart: bool = Field(default=False, description="statement will be restarted, this is to differentiate child treatment from parent")
    version: str = Field(default="", description="Version of the table, used to identify modified statements")

    def add_child(self, child):
        self.children.add(child)
        child.parents.add(self)

    def add_parent(self, parent):
        self.parents.add(parent)
        parent.children.add(self)

    def is_running(self) -> bool:
        if self.existing_statement_info and self.existing_statement_info.status_phase:
            return (self.existing_statement_info.status_phase in ["RUNNING", "PENDING", "COMPLETED"])
        else:
            return False

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkStatementNode):
            return NotImplemented
        return self.table_name == other.table_name

    def __hash__(self) -> int:
        return hash(self.table_name)

class FlinkStatementExecutionPlan(BaseModel):
    """
    Execution plan from the current start table to all the children and parents not already deployed.
    The start node is part of the nodes list.
    The nodes list is sorted by the order of execution
    """
    created_at: datetime = Field(default=datetime.now())
    start_table_name: str = Field(default=None)
    environment_id: str = Field(default=None)
    nodes: List[FlinkStatementNode] = Field(default=[])


