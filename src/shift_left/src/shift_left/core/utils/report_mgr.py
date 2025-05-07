from pydantic import BaseModel
from typing import List, Optional
from shift_left.core.models.flink_statement_model import Statement, FlinkStatementExecutionPlan
from shift_left.core.models.flink_compute_pool_model import ComputePoolInfo, ComputePoolList
from shift_left.core.compute_pool_mgr import get_compute_pool_with_id
from shift_left.core.utils.app_config import shift_left_dir
from pydantic import Field

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
    compute_pool_id: Optional[str] = None
    ddl_dml: str = Field(default="Both", description="Type of deployment: DML only, or both")
    update_children: bool = Field(default=False)
    flink_statements_deployed: List[Statement] = Field(default_factory=list)

class TableInfo(BaseModel):
    table_name: str = ""
    type: str = ""
    upgrade_mode: str = ""
    statement_name: str = ""
    status: str = ""
    created_at: str = ""
    compute_pool_id: str = ""
    compute_pool_name: str = ""

class TableReport(BaseModel):
    product_name: str = ""
    environment_id: str = ""
    catalog_name: str = ""
    database_name: str = ""
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
    if len(text) > length:
        return text[:length]
    else:
        return text.ljust(length, padding_char)
    
def build_simple_report(execution_plan: FlinkStatementExecutionPlan) -> str:
    report = f"{pad_or_truncate('Ancestor Table Name',50)}\t{pad_or_truncate('Statement Name', 40)}\t{'Status':<10}\t{'Compute Pool':<15}\n"
    report+=f"-"*130 + "\n"
    for node in execution_plan.nodes:
        if node.existing_statement_info:
            report+=f"{pad_or_truncate(node.table_name, 50)}\t{pad_or_truncate(node.dml_statement_name, 40)}\t{pad_or_truncate(node.existing_statement_info.status_phase,10)}\t{pad_or_truncate(node.compute_pool_id,15)}\n"
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
        f"To deploy {execution_plan.start_table_name} to {execution_plan.environment_id}, the following statements need to be executed in the order\n"
    ]
    
    # Separate nodes into parents and children
    parents = [node for node in execution_plan.nodes if (node.to_run or node.is_running()) and not node.to_restart]
    children = [node for node in execution_plan.nodes if node.to_restart]
    
    # Build parent section
    if parents:
        summary_parts.extend([
            "\n--- Ancestors impacted ---",
            "Statement Name".ljust(60) + "\tStatus\t\tCompute Pool\tAction\tUpgrade Mode\tTable Name",
            "-" * 125
        ])
        for node in parents:
            action = "Run" if node.to_run else "Skip"
            if node.to_restart:
                action= "Restart"
            status_phase = node.existing_statement_info.status_phase if node.existing_statement_info else "Not deployed"
            summary_parts.append(
                f"{pad_or_truncate(node.dml_statement_name,60)}\t{status_phase[:7]}\t\t{node.compute_pool_id}\t{action}\t{node.upgrade_mode}\t{node.table_name}"
            )
    
    # Build children section
    if children:
        summary_parts.extend([
            f"\n--- {len(parents)} parents",
            "--- Children to restart ---",
            "Statement Name".ljust(60) + "\tStatus\t\tCompute Pool\tAction\tUpgrade Mode\tTable Name",
            "-" * 125
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
    summary+=f"\nPool ID   \t{pad_or_truncate('Name',40)}\tCurrent/Max CFU\tFlink Statement"
    for node in execution_plan.nodes:
        pool = get_compute_pool_with_id(compute_pool_list, node.compute_pool_id)
        if pool:
            summary+=f"\n{pool.id} \t{pad_or_truncate(pool.name,40)}\t{pad_or_truncate(str(pool.current_cfu) + '/' + str(pool.max_cfu),10)}\t{node.dml_statement_name}"
    with open(shift_left_dir + f"/{execution_plan.start_table_name}_summary.txt", "w") as f:
        f.write(summary)
    return summary
