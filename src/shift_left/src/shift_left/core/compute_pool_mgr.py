"""
Copyright 2024-2025 Confluent, Inc.
"""
import time
import os, json
from importlib import import_module
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.statement_mgr import get_statement_info
from shift_left.core.pipeline_mgr import FlinkTablePipelineDefinition
from shift_left.core.flink_compute_pool_model import *
from shift_left.core.utils.naming_convention import ComputePoolNameModifier
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import ( 
    get_ddl_dml_names_from_table, 
    extract_product_name
)

STATEMENT_COMPUTE_POOL_FILE=shift_left_dir + "/pool_assignments.json"
COMPUTE_POOL_LIST_FILE=shift_left_dir + "/compute_pool_list.json"


def get_or_build_compute_pool(compute_pool_id: str, pipeline_def: FlinkTablePipelineDefinition):
    """
    if the compute pool is given, use it, else assess if there is a statement
    for this table already running and reuse the compute pool, if not
    reuse the compute pool persisted in the table's pipeline definition.
    else create a new pool.
    """
    config = get_config()
    if not compute_pool_id:
        # Before using the config compute pool, do we want to get the compute pool associated to the statement?
        compute_pool_id = config['flink']['compute_pool_id']
    logger.info(f"Validate the {compute_pool_id} exists and has enough resources")

    client = ConfluentCloudClient(config)
    env_id = config['confluent_cloud']['environment_id']
    if compute_pool_id and _validate_a_pool(client, compute_pool_id, env_id):
        return compute_pool_id
    else:
        logger.info(f"Look at the compute pool, currently used by {pipeline_def.dml_ref} by querying statement")
        product_name = extract_product_name(pipeline_def.path)
        _, dml_statement_name = get_ddl_dml_names_from_table(pipeline_def.table_name)
        statement = get_statement_info(dml_statement_name)
        if statement and statement.spec.compute_pool_id:
            pool_id= statement.spec.compute_pool_id
            return pool_id
        else:
            logger.info(f"Build a new compute pool")
            return _create_compute_pool(pipeline_def.table_name)


_compute_pool_list = None
def get_compute_pool_list(env_id: str = None, region: str = None) -> ComputePoolList:
    global _compute_pool_list
    config = get_config()
    if not env_id:
        env_id = config['confluent_cloud']['environment_id']
    if not region:
        region = config['confluent_cloud']['region']
    if not _compute_pool_list:
        reload = True
        if os.path.exists(COMPUTE_POOL_LIST_FILE):
            with open(COMPUTE_POOL_LIST_FILE, "r") as f:
                _compute_pool_list = ComputePoolList.model_validate_json(f.read())
            if _compute_pool_list.created_at and (datetime.now() - datetime.fromisoformat(_compute_pool_list.created_at)).total_seconds() < config['app']['cache_ttl']:  
                # keep the list if it was created in the last 60 minutes
                reload = False
        if reload:
            logger.info(f"Get the compute pool list for environment {env_id}, {region} using API")
            client = ConfluentCloudClient(get_config())
            response: ComputePoolListResponse = client.get_compute_pool_list(env_id, region)
            _compute_pool_list = ComputePoolList(created_at=datetime.now().isoformat())
            for pool in response.data:
                cp_pool = ComputePoolInfo(id=pool.id,
                                        name=pool.spec.display_name, 
                                        env_id=pool.spec.environment.id,
                                        max_cfu=pool.spec.max_cfu,
                                        region=pool.spec.region,
                                        status_phase=pool.status.phase,
                                        current_cfu=pool.status.current_cfu)
                _compute_pool_list.pools.append(cp_pool)
            _save_compute_pool_list(_compute_pool_list)
            logger.debug(f"Compute pool list has {len(_compute_pool_list.pools)} pools")
    return _compute_pool_list



def save_compute_pool_info_in_metadata(statement_name, compute_pool_id: str):
    data = {}
    if os.path.exists(STATEMENT_COMPUTE_POOL_FILE):
        with open(STATEMENT_COMPUTE_POOL_FILE, "r")  as f:
            data=json.load(f) 
    data[statement_name] = {"statement_name": statement_name, "compute_pool_id": compute_pool_id}
    with open(STATEMENT_COMPUTE_POOL_FILE, "w") as f:
        json.dump(data, f, indent=4)

def search_for_matching_compute_pools(compute_pool_list: ComputePoolList, table_name: str) -> List[ComputePoolInfo]:
    matching_pools = []
    _target_pool_name = _get_compute_pool_name_modifier().build_compute_pool_name_from_table(table_name)
    for pool in compute_pool_list.pools:
        if _target_pool_name == pool.name:
            matching_pools.append(pool)
    if len(matching_pools) == 0:
        logger.info(f"The target pool name {_target_pool_name} does not match any compute pool")
    return matching_pools

def get_compute_pool_with_id(compute_pool_list: ComputePoolList, compute_pool_id: str) -> ComputePoolInfo:
    for pool in compute_pool_list.pools:
        if pool.id == compute_pool_id:
            return pool
    return None

def is_pool_valid(compute_pool_id):
    """
    Returns whether the supplied compute pool id is valid
    """
    config = get_config()
    logger.info(f"Validate the {compute_pool_id} exists and has enough resources")

    client = ConfluentCloudClient(config)
    env_id = config['confluent_cloud']['environment_id']
    return compute_pool_id and _validate_a_pool(client, compute_pool_id, env_id)

# ------ Private methods ------


def _save_compute_pool_list(compute_pool_list: ComputePoolList):
    with open(COMPUTE_POOL_LIST_FILE, "w") as f:
        json.dump(compute_pool_list.model_dump(), f, indent=4)

def _create_compute_pool(table_name: str) -> str:
    config = get_config()
    spec = _build_compute_pool_spec(table_name, config)
    client = ConfluentCloudClient(get_config())
    result= client.create_compute_pool(spec)
    if result:
        pool_id = result['id']
        env_id = config['confluent_cloud']['environment_id']
        if _verify_compute_pool_provisioned(client, pool_id, env_id):
            return pool_id


def _build_compute_pool_spec(table_name: str, config: dict) -> dict:
    spec = {}
    spec['display_name'] = _get_compute_pool_name_modifier().build_compute_pool_name_from_table(table_name)
    spec['cloud'] = config['confluent_cloud']['provider']
    spec['region'] = config['confluent_cloud']['region']
    spec['max_cfu'] =  config['flink']['max_cfu']
    spec['environment'] = { 'id': config['confluent_cloud']['environment_id']}
    return spec

def _verify_compute_pool_provisioned(client, pool_id: str, env_id: str) -> bool:
    """
    Wait for the compute pool is provisionned
    """
    provisioning = True
    failed = False
    while provisioning:
        logger.info("Wait ...")
        time.sleep(5)
        result= client.get_compute_pool_info(pool_id, env_id)
        provisioning = (result['status']['phase'] == "PROVISIONING")
        failed = (result['status']['phase'] == "FAILED")
    return False if failed else True


def _get_pool_usage(pool_info: dict) -> float:
    current = pool_info['status']['current_cfu']
    max = pool_info['spec']['max_cfu']
    return (current / max)

def _validate_a_pool(client: ConfluentCloudClient, compute_pool_id: str, env_id: str) -> bool:
    """
    Validate a pool exist and with enough resources
    """
    try:
        pool_info=client.get_compute_pool_info(compute_pool_id, env_id)
        if (
            pool_info == None
            or
            (
                "errors" in pool_info
                and
                # Including 403 because there's a bug in Confluent
                #  where it returns 403 instead of 404 for missing resources.
                any(map(
                    lambda e:"status" in e and int(e["status"]) in [403,404],
                    pool_info["errors"],
                ))
            )
        ):
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
        return False




_compute_pool_name_modifier = None
def _get_compute_pool_name_modifier():
    global _compute_pool_name_modifier
    if not _compute_pool_name_modifier:
        if get_config().get('app').get('compute_pool_naming_convention_modifier'):
            class_to_use = get_config().get('app').get('compute_pool_naming_convention_modifier')
            module_path, class_name = class_to_use.rsplit('.',1)
            mod = import_module(module_path)
            _compute_pool_name_modifier = getattr(mod, class_name)()
        else:
            _compute_pool_name_modifier = ComputePoolNameModifier()
    return _compute_pool_name_modifier