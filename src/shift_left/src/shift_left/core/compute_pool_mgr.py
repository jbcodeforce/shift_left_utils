import time
import os, json
from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import get_statement
from shift_left.core.pipeline_mgr import FlinkTablePipelineDefinition
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import ( 
    get_ddl_dml_names_from_table, 
    extract_product_name
)

STATEMENT_COMPUTE_POOL_FILE=os.getenv("PIPELINES") + "/pool_assignments.json"

def get_or_build_compute_pool(compute_pool_id: str, pipeline_def: FlinkTablePipelineDefinition):
    """
    if the compute pool is given, use it, else assess if there is a statement
    for this table already running and reuse the compute pool, if not
    reuse the compute pool persisted in the table's pipeline definition.
    else create a new pool.
    """
    config = get_config()
    if not compute_pool_id:
        compute_pool_id = config['flink']['compute_pool_id']
    logger.info(f"Validate the {compute_pool_id} exists and has enough resources")

    client = ConfluentCloudClient(config)
    if compute_pool_id and _validate_a_pool(client, compute_pool_id):
        return compute_pool_id
    else:
        logger.info(f"Look at the compute pool, currently used by {pipeline_def.dml_ref} by querying statement")
        product_name = extract_product_name(pipeline_def.path)
        _, dml_statement_name = get_ddl_dml_names_from_table(pipeline_def.table_name, 
                                                    config['kafka']['cluster_type'], 
                                                    product_name)
        statement = get_statement(dml_statement_name)
        pool_id= statement.spec.compute_pool_id
        if pool_id:
            return pool_id
        else:
            logger.info(f"Build a new compute pool")
            pool_spec = _build_compute_pool_spec(pipeline_def.table_name)
            return _create_compute_pool(pool_spec)


def save_compute_pool_info_in_metadata(statement_name, compute_pool_id: str):
    data = {}
    if os.path.exists(STATEMENT_COMPUTE_POOL_FILE):
        with open(STATEMENT_COMPUTE_POOL_FILE, "r")  as f:
            data=json.load(f) 
    data[statement_name] = {"statement_name": statement_name, "compute_pool_id": compute_pool_id}
    with open(STATEMENT_COMPUTE_POOL_FILE, "w") as f:
        json.dump(data, f, indent=4)


def _create_compute_pool(table_name: str) -> str:
    config = get_config()
    spec = _build_compute_pool_spec(table_name, config)
    client = ConfluentCloudClient(get_config())
    result= client.create_compute_pool(client,spec)
    if result:
        pool_id = result['id']
        _verify_compute_pool_provisioned(pool_id)


def _build_compute_pool_spec(table_name: str, config: dict) -> dict:
    spec = {}
    spec['display_name'] = "cp-" + table_name.replace('_','-')
    spec['cloud'] = config['confluent_cloud']['provider']
    spec['region'] = config['confluent_cloud']['region']
    spec['max_cfu'] =  config['flink']['max_cfu']
    spec['environment'] = { 'id': config['confluent_cloud']['environment_id']}
    return spec

def _verify_compute_pool_provisioned(client, pool_id: str) -> bool:
    """
    Wait for the compute pool is provisionned
    """
    provisioning = True
    failed = False
    while provisioning:
        logger.info("Wait ...")
        time.sleep(5)
        result= client.get_compute_pool_info(pool_id)
        provisioning = (result['status']['phase'] == "PROVISIONING")
        failed = (result['status']['phase'] == "FAILED")
    return False if failed else True


def _get_pool_usage(pool_info: dict) -> float:
    current = pool_info['status']['current_cfu']
    max = pool_info['spec']['max_cfu']
    return (current / max)

def _validate_a_pool(client: ConfluentCloudClient, compute_pool_id: str) -> bool:
    """
    Validate a pool exist and with enough resources
    """
    try:
        pool_info=client.get_compute_pool_info(compute_pool_id)
        if pool_info == None:
            logger.info(f"Compute Pool not found")
            raise Exception(f"The given compute pool {compute_pool_id} is not found, will use parameter or config.yaml one")
        logger.info(f"Using compute pool {compute_pool_id} with {pool_info['status']['current_cfu']} CFUs for a max: {pool_info['spec']['max_cfu']} CFUs")
        ratio = _get_pool_usage(pool_info) 
        if ratio >= 0.7:
            raise Exception(f"The CFU usage at {ratio} % is too high for {compute_pool_id}")
        return pool_info['status']['phase'] == "PROVISIONED"
    except Exception as e:
        logger.error(e)
        logger.info("Continue processing to ignore compute pool constraint")
        return True
