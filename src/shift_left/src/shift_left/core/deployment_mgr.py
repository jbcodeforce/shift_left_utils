
from shift_left.core.pipeline_mgr import ReportInfoNode   
from shift_left.core.utils.ccloud_client import ConfluentFlinkClient

def _get_current_compute_pool_cfu(compute_pool_id: str, environment_id: str):
    """
    Get the current compute pool CFU
    """
    # TODO: implement
    url = f"/fcpm/v2/compute-pools/{compute_pool_id}?environment={environment_id}"
    client = ConfluentFlinkClient()
    result= client.get_commpute_pool_info()
    max_cfu=result["spec"]["max_cfu"]
    current_cfu=result["status"]["current_cfu"]

    return max_cfu


def _deploy_ddl_statements(ddl_path: str):
    print(f"Deploying DDL statements from {ddl_path}")
    # TODO: implement
def _deploy_dml_statements(dml_path: str):
    print(f"Deploying DML statements from {dml_path}")
    # TODO: implement

def deploy_pipeline_from_table(pipeline_node_data: ReportInfoNode):
    """
    Deploy a pipeline from a given folder
    """
    _deploy_ddl_statements(pipeline_node_data.ddl_path)
    _deploy_dml_statements(pipeline_node_data.dml_path)
    for child in pipeline_node_data.children:
        _deploy_ddl_statements(child.ddl_path)