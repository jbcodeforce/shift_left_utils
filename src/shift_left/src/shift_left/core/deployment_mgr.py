
from shift_left.core.pipeline_mgr import PipelineNodeData   



def _deploy_ddl_statements(ddl_path: str):
    print(f"Deploying DDL statements from {ddl_path}")
    # TODO: implement
def _deploy_dml_statements(dml_path: str):
    print(f"Deploying DML statements from {dml_path}")
    # TODO: implement

def deploy_pipeline_from_table(pipeline_node_data: PipelineNodeData):
    """
    Deploy a pipeline from a given folder
    """
    _deploy_ddl_statements(pipeline_node_data.ddl_path)
    _deploy_dml_statements(pipeline_node_data.dml_path)
    for child in pipeline_node_data.children:
        _deploy_ddl_statements(child.ddl_path)