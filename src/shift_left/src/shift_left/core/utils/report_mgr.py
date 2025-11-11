from pydantic import BaseModel
from typing import List, Optional
from shift_left.core.models.flink_statement_model import Statement, FlinkStatementExecutionPlan, FlinkStatementNode
from shift_left.core.models.flink_compute_pool_model import ComputePoolList

import shift_left.core.metric_mgr as metrics_mgr
import shift_left.core.utils.report_mgr as report_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
from shift_left.core.utils.app_config import shift_left_dir, get_config, logger
from pydantic import Field
from datetime import datetime
import time

class StatementBasicInfo(BaseModel):
    name: str
    environment_id: str
    created_at: Optional[datetime] = Field(default=None)
    uid: str
    compute_pool_id: str
    status: str
    status_details: str
    start_time: float = 0
    execution_time: float =0

class DeploymentReport(BaseModel):
    """Report of a pipeline deployment operation.

    Attributes:
        table_name: Name of the table being deployed
        compute_pool_id: ID of the compute pool used
        ddl_dml: Type of deployment (DML only or both)
        update_children: Whether to update child pipelines
        flink_statements_deployed: List of deployed Flink statements
    """
    table_name: Optional[str] = None
    type: str = Field(default="Both", description="Type of deployment: DML only, or both")
    update_children: bool = Field(default=False)
    start_time: float = 0
    execution_time: float = 0
    flink_statements_deployed: List[StatementBasicInfo] = Field(default_factory=list)

class TableInfo(BaseModel):
    table_name: str = ""
    type: str = ""
    upgrade_mode: str = ""
    statement_name: str = ""
    status: str = ""
    created_at: Optional[datetime] = Field(default=None)
    compute_pool_id: str = ""
    compute_pool_name: str = ""
    to_restart: bool = False
    to_run: bool = False
    retention_size: int = 0
    message_count: int = 0
    pending_records: float = 0
    num_records_out: int = 0
    num_records_in: int = 0

class TableReport(BaseModel):
    report_name: str = ""
    environment_id: str = ""
    catalog_name: str = ""
    database_name: str = ""
    created_at: Optional[datetime] = Field(default=None)
    tables: List[TableInfo] = []

def pad_or_truncate(text: str, length: int, padding_char: str = ' ') -> str:
    """
    Pad or truncate text to a specific length.

    Args:
        text: Text to pad/truncate
        length: Target length
        padding_char: Character to use for padding

    Returns:
        Padded or truncated text
    """
    if isinstance(text, str):
        if len(text) > length:
            return text[:length]
        else:
            return text.ljust(length, padding_char)
    else:
        return str(text).ljust(length, padding_char)

def build_TableReport(report_name: str,
                      nodes: List[FlinkStatementNode],
                      from_date: str,
                      get_metrics: bool = False) -> TableReport:
    table_report = TableReport()
    table_report.report_name = report_name
    table_report.environment_id = get_config().get('confluent_cloud').get('environment_id')
    table_report.catalog_name = get_config().get('flink').get('catalog_name')
    table_report.database_name = get_config().get('flink').get('database_name')
    table_report.created_at = datetime.now()
    if from_date:
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Building table report for {report_name} with {len(nodes)} nodes for {from_date}")
    else:
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Building table report for {report_name} with {len(nodes)} nodes from now")
    if get_metrics:
        compute_pool_ids = [node.compute_pool_id for node in nodes]
        pending_records = metrics_mgr.get_pending_records(compute_pool_ids,from_date=from_date)
        num_records_out = metrics_mgr.get_num_records_out(compute_pool_ids,from_date=from_date)
        num_records_in = metrics_mgr.get_num_records_in(compute_pool_ids,from_date=from_date)
        for node in nodes:
            table_info = build_TableInfo(node,get_metrics=get_metrics)
            if node.existing_statement_info:
                table_info.pending_records = pending_records.get(node.existing_statement_info.name, 0)
                table_info.num_records_out = num_records_out.get(node.existing_statement_info.name, 0)
                table_info.num_records_in = num_records_in.get(node.existing_statement_info.name, 0)
            else:
                logger.error(f"Node {node.table_name} has no existing statement info")
                table_info.pending_records = 0
                table_info.num_records_out = 0
                table_info.num_records_in = 0
            table_report.tables.append(table_info)
    else:
        for node in nodes:
            table_info = build_TableInfo(node, get_metrics=get_metrics)
            table_report.tables.append(table_info)
    return table_report

def build_TableInfo(node: FlinkStatementNode, get_metrics: bool = False) -> TableInfo:
    table_info = TableInfo()
    table_info.table_name = node.table_name
    table_info.type = node.type
    table_info.upgrade_mode = node.upgrade_mode
    table_info.statement_name = node.dml_statement_name
    table_info.to_restart = node.to_restart
    table_info.to_run = node.to_run
    compute_pool_list = compute_pool_mgr.get_compute_pool_list()
    if node.existing_statement_info:
        table_info.status = node.existing_statement_info.status_phase
        table_info.compute_pool_id = node.existing_statement_info.compute_pool_id
        table_info.created_at = node.existing_statement_info.created_at
        table_info.statement_name = node.existing_statement_info.name
    else:
        table_info.status = "UNKNOWN"
        table_info.compute_pool_id = ""
        table_info.created_at = datetime.now()
    pool = compute_pool_mgr.get_compute_pool_with_id(compute_pool_list, table_info.compute_pool_id)
    if pool:
        table_info.compute_pool_name = pool.name
    else:
        table_info.compute_pool_name = "UNKNOWN"
    if table_info.status == "RUNNING" and get_metrics:
        table_info.retention_size = metrics_mgr.get_retention_size(table_info.table_name)
        #table_info.message_count = metrics_mgr.get_total_amount_of_messages(table_info.table_name, compute_pool_id=table_info.compute_pool_id)

    return table_info

def build_simple_report(execution_plan: FlinkStatementExecutionPlan) -> str:
    report = f"{pad_or_truncate('Ancestor Table Name',40)}\t{pad_or_truncate('Statement Name', 40)} {'Status':<10} {'Compute Pool':<15} {'Created At':<16} {'Pending_msgs':<12} {'Num_msg_in':<11} {'Num_msg_out':<11}\n"
    report+=f"-"*165 + "\n"
    compute_pool_ids = [node.compute_pool_id for node in execution_plan.nodes]
    pending_records = metrics_mgr.get_pending_records(compute_pool_ids,from_date="")
    num_records_out = metrics_mgr.get_num_records_out(compute_pool_ids,from_date="")
    num_records_in = metrics_mgr.get_num_records_in(compute_pool_ids,from_date="")
    for node in execution_plan.nodes:
        if node.existing_statement_info:
            pending_records_value = pending_records.get(node.existing_statement_info.name, 0)
            num_records_out_value = num_records_out.get(node.existing_statement_info.name, 0)
            num_records_in_value = num_records_in.get(node.existing_statement_info.name, 0)
            report+=f"{pad_or_truncate(node.table_name, 40)}\t{pad_or_truncate(node.dml_statement_name, 40)} {pad_or_truncate(node.existing_statement_info.status_phase,10)} {pad_or_truncate(node.compute_pool_id,15)} {pad_or_truncate(node.created_at.strftime('%Y-%m-%d %H:%M:%S'),16)}\t{pad_or_truncate(pending_records_value,12)} {pad_or_truncate(num_records_in_value,11)} {pad_or_truncate(num_records_out_value,11)}\n"
    return report



def build_summary_from_execution_plan(execution_plan: FlinkStatementExecutionPlan, compute_pool_list: ComputePoolList) -> str:
    """
    Build a summary of the execution plan showing which statements need to be executed.

    Args:
        execution_plan: The execution plan containing nodes to be processed

    Returns:
        A formatted string summarizing the execution plan
    """


    summary_parts = [
        f"\nTo deploy {execution_plan.start_table_name} to {execution_plan.environment_id}, the following statements need to be executed in the order\n"
    ]

    # Separate nodes into parents and children
    parents = [node for node in execution_plan.nodes if (node.to_run or node.is_running())]
    children = [node for node in execution_plan.nodes if node.to_restart]

    # Build parent section
    if parents:
        summary_parts.extend([
            f"\n--- Ancestors: {len(parents)} ---",
            "Statement Name".ljust(60) + "\tStatus\t\tCompute Pool\tAction\tUpgrade Mode\tTable Name",
            "-" * 155
        ])
        for node in parents:
            action = "To run" if node.to_run else "Skip"
            if node.to_restart:
                action= "Restart"
            status_phase = node.existing_statement_info.status_phase if node.existing_statement_info else "Not deployed"
            summary_parts.append(
                f"{pad_or_truncate(node.dml_statement_name,60)}\t{status_phase[:7]}\t\t{node.compute_pool_id}\t{action}\t{node.upgrade_mode}\t{node.table_name}"
            )

    # Build children section
    if children:
        summary_parts.extend([
            f"\n--- Children to restart ---",
            "Statement Name".ljust(60) + "\tStatus\t\tCompute Pool\tAction\tUpgrade Mode\tTable Name",
            "-" * 155
        ])
        for node in children:
            action = "To run" if node.to_run else "Restart" if node.to_restart else "Skip"
            status_phase = node.existing_statement_info.status_phase if node.existing_statement_info else "Not deployed"

            summary_parts.append(
                f"{pad_or_truncate(node.dml_statement_name,60)}\t{status_phase[:7]}\t\t{node.compute_pool_id}\t{action}\t{node.upgrade_mode}\t{node.table_name}"
            )
        summary_parts.append(f"--- {len(children)} children to restart")

    summary= "\n".join(summary_parts)
    summary+="\n---Matching compute pools: "
    summary+=f"\nPool ID   \t{pad_or_truncate('Pool Name',40)}\tCurrent/Max CFU\tFlink Statement name\n" + "-" * 140
    for node in execution_plan.nodes:
        pool = compute_pool_mgr.get_compute_pool_with_id(compute_pool_list, node.compute_pool_id)
        if pool:
            summary+=f"\n{pool.id} \t{pad_or_truncate(pool.name,40)}\t{pad_or_truncate(str(pool.current_cfu) + '/' + str(pool.max_cfu),10)}\t{node.dml_statement_name}"
    with open(shift_left_dir + f"/{execution_plan.start_table_name}_summary.txt", "w") as f:
        f.write(summary)
    return summary


def build_deployment_report(
    table_name: str,
    dml_ref: str,
    may_start_children: bool,
    statements: List[Statement]) -> DeploymentReport:
    report = DeploymentReport(table_name=table_name, type=dml_ref, update_children=may_start_children)
    for statement in statements:
        if statement:
            report.flink_statements_deployed.append(_build_statement_basic_info(statement))
    return report

def persist_table_reports(table_report: TableReport, base_file_name):
    table_count=0
    running_count=0
    non_running_count=0
    csv_content= "environment_id,catalog_name,database_name,table_name,type,upgrade_mode,statement_name,status,compute_pool_id,compute_pool_name,created_at,retention_size,message_count,pending_records,num_records_in,num_records_out\n"
    for table in table_report.tables:
        csv_content+=f"{table_report.environment_id},{table_report.catalog_name},{table_report.database_name},{table.table_name},{table.type},{table.upgrade_mode},{table.statement_name},{table.status},{table.compute_pool_id},{table.compute_pool_name},{table.created_at.strftime('%Y-%m-%d %H:%M:%S')},{table.retention_size},{table.message_count},{table.pending_records},{table.num_records_in},{table.num_records_out}\n"
        if table.status == 'RUNNING':
            running_count+=1
        else:
            non_running_count+=1
        print(f"Table info: {report_mgr.pad_or_truncate(table.table_name, 40)} {report_mgr.pad_or_truncate(table.status, 10)} created: {report_mgr.pad_or_truncate(table.created_at.strftime('%Y-%m-%dT%H:%M:%S'), 20)} pool: {report_mgr.pad_or_truncate(table.compute_pool_id, 10)} pending records: {table.pending_records} sum records in: {table.num_records_in} sum records out: {table.num_records_out}")

        table_count+=1
    print(f"Writing report to {shift_left_dir}/{base_file_name}_report.csv and {shift_left_dir}/{base_file_name}_report.json")
    with open(f"{shift_left_dir}/{base_file_name}_report.csv", "w") as f:
        f.write(csv_content)
    with open(f"{shift_left_dir}/{base_file_name}_report.json", "w") as f:
        f.write(table_report.model_dump_json(indent=4))
    result=f"#"*120 + "\n\tEnvironment: " + get_config()['confluent_cloud']['environment_id'] + "\n"
    result+=f"\tCatalog: " + get_config()['flink']['catalog_name'] + "\n"
    result+=f"\tDatabase: " + get_config()['flink']['database_name'] + "\n"
    result+=csv_content
    result+="#"*120 + f"\n\tRunning tables: {running_count}" + "\n"
    result+=f"\tNon running tables: {non_running_count}" + "\n"
    return result

def _build_statement_basic_info(statement: Statement) -> StatementBasicInfo:
    if statement.status and statement.status.detail:
        status_detail = statement.status.detail
    else:
        status_detail = ""

    status_phase = statement.status.phase if statement.status else "UNKNOWN"

    return StatementBasicInfo(
        name=statement.name,
        environment_id=statement.environment_id,
        created_at=statement.metadata.created_at,
        uid=statement.metadata.uid,
        compute_pool_id=statement.spec.compute_pool_id,
        status=status_phase,
        status_details=status_detail,
        execution_time=statement.execution_time
    )
