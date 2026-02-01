"""
Copyright 2024-2025 Confluent, Inc.
"""
import time
import os, json
from importlib import import_module
from typing import Tuple
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir, session_log_dir
from shift_left.core.statement_mgr import get_statement_info
from shift_left.core.pipeline_mgr import FlinkTablePipelineDefinition
from shift_left.core.models.flink_compute_pool_model import *
from shift_left.core.utils.naming_convention import ComputePoolNameModifier
from shift_left.core.utils.ccloud_client import ConfluentCloudClient


STATEMENT_COMPUTE_POOL_FILE=session_log_dir + "/pool_assignments.json"
COMPUTE_POOL_LIST_FILE=session_log_dir + "/compute_pool_list.json"


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
                _compute_pool_list = ComputePoolList.model_validate(json.load(f))
            if _compute_pool_list.created_at and (datetime.now() - _compute_pool_list.created_at).total_seconds() < config['app']['cache_ttl']:
                # keep the list if it was created in the last 60 minutes
                reload = False
        if reload:
            logger.info(f"Get the compute pool list for environment {env_id}, {region} using API {get_config().get('confluent_cloud').get('api_key')}")
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Get the compute pool list for environment [{env_id}] Region [{region}] API_Key [{get_config().get('confluent_cloud').get('api_key')}]")
            client = ConfluentCloudClient(get_config())
            response: ComputePoolListResponse = client.get_compute_pool_list(env_id, region)
            _compute_pool_list = ComputePoolList(created_at=datetime.now())
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
            logger.info(f"Compute pool list has {len(_compute_pool_list.pools)} compute pools")
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Compute pool list has {len(_compute_pool_list.pools)} compute pools")
    elif (_compute_pool_list.created_at
         and (datetime.now() - _compute_pool_list.created_at).total_seconds() > get_config()['app']['cache_ttl']):
        logger.info("Compute pool list cache is expired, reload it")
        _compute_pool_list = None
        return get_compute_pool_list(env_id, region)
    return _compute_pool_list

def reset_compute_list():
    global _compute_pool_list
    _compute_pool_list = None


def save_compute_pool_info_in_metadata(statement_name, compute_pool_id: str):
    data = {}
    logger.info(f"Save compute pool info in metadata file {STATEMENT_COMPUTE_POOL_FILE}")
    if os.path.exists(STATEMENT_COMPUTE_POOL_FILE):
        with open(STATEMENT_COMPUTE_POOL_FILE, "r")  as f:
            data=json.load(f)
    data[statement_name] = {"statement_name": statement_name, "compute_pool_id": compute_pool_id}
    with open(STATEMENT_COMPUTE_POOL_FILE, "w") as f:
        json.dump(data, f, indent=4)

def search_for_matching_compute_pools(table_name: str) -> List[ComputePoolInfo]:
    matching_pools = []
    compute_pool_list = get_compute_pool_list()
    _target_pool_name = _get_compute_pool_name_modifier().build_compute_pool_name_from_table(table_name)
    logger.info(f"Target pool name: {_target_pool_name}")
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

def get_compute_pool_name(compute_pool_id: str):
    compute_pool_list = get_compute_pool_list()
    pool = get_compute_pool_with_id(compute_pool_list, compute_pool_id)
    if pool:
        compute_pool_name = pool.name
    else:
        compute_pool_name = "UNKNOWN"
    return compute_pool_name

def is_pool_valid(compute_pool_id) -> bool:
    """
    Returns whether the supplied compute pool id is valid
    """
    config = get_config()
    logger.debug(f"Validate the {compute_pool_id} exists and has enough resources")
    compute_pool_list = get_compute_pool_list()
    for pool in compute_pool_list.pools:
        if pool.id == compute_pool_id:
            ratio = get_pool_usage_from_pool_info(pool)
            if ratio >= config['flink'].get('max_cfu_percent_before_allocation', .7):
                raise Exception(f"The CFU usage at {ratio} % is too high for {compute_pool_id}")
            return True
    client = ConfluentCloudClient(config)
    env_id = config['confluent_cloud']['environment_id']
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
        ratio = get_pool_usage_from_dict(pool_info)
        if ratio >= config['flink'].get('max_cfu_percent_before_allocation', .7):
            raise Exception(f"The CFU usage at {ratio} % is too high for {compute_pool_id}")
        return pool_info['status']['phase'] == "PROVISIONED"
    except Exception as e:
        logger.warning(e)
        logger.info("Continue processing using another compute pool from parameter or config.yaml")
        return False


def create_compute_pool(table_name: str) -> Tuple[str, str]:
    config = get_config()
    spec = _build_compute_pool_spec(table_name, config)
    logger.info(f"Create compute pool {spec['display_name']} for {table_name} ... it may take a while")
    print(f"Create compute pool {spec['display_name']} for {table_name} ... it may take a while")
    client = ConfluentCloudClient(get_config())
    try:
        result= client.create_compute_pool(spec)
        if result and not result.get('errors'):
            pool_id = result['id']
            env_id = config['confluent_cloud']['environment_id']
            if _verify_compute_pool_provisioned(client, pool_id, env_id):
                logger.info(f"Compute pool {pool_id} created and provisioned")
                get_compute_pool_list().pools.append(ComputePoolInfo(
                                        id=pool_id,
                                        name=result['spec']['display_name'],
                                        env_id=env_id,
                                        max_cfu=result['spec']['max_cfu'],
                                        region=result['spec']['region'],
                                        status_phase=result['status']['phase'],
                                        current_cfu=result['status']['current_cfu']))
                return pool_id, result['spec']['display_name']
        elif result.get('errors').get('status') == "409":
            logger.error(f"Compute pool {spec['display_name']} already exists")
            return pool_id, spec['display_name']
        else:
            logger.error(f"Error creating compute pool: {result.get('errors')} -> using the one in config.yaml")
            return config['flink']['compute_pool_id'], spec['display_name']
    except Exception as e:
        logger.error(e)
        raise e

def delete_compute_pool(compute_pool_id: str):
    logger.info(f"Delete the compute pool {compute_pool_id}")
    config = get_config()
    env_id = config['confluent_cloud']['environment_id']
    client = ConfluentCloudClient(config)
    client.delete_compute_pool(compute_pool_id, env_id)
    _cp_list = get_compute_pool_list()
    _cp_list.pools.remove(get_compute_pool_with_id(_cp_list, compute_pool_id))
    _save_compute_pool_list(_cp_list)

def get_pool_usage_from_dict(pool_info: dict) -> float:
    current = pool_info['status']['current_cfu']
    max = pool_info['spec']['max_cfu']
    return (current / max)

def get_pool_usage_from_pool_info(pool_info: ComputePoolInfo) -> float:
    current = pool_info.current_cfu
    max = pool_info.max_cfu
    return (current / max)

def delete_all_compute_pools_of_product(product_name: str):
    if product_name:
        compute_pool_list = get_compute_pool_list()
        logger.info(f"Delete all compute pools for product {product_name}")

        # First, collect all pools that need to be deleted
        pools_to_delete = []
        for pool in compute_pool_list.pools:
            if product_name in pool.name:
                pools_to_delete.append(pool)

        # Then delete them in a separate loop
        count = 0
        for pool in pools_to_delete:
            delete_compute_pool(pool.id)
            print(f"Deleted compute pool {pool.id} for product {product_name}")
            count += 1

        logger.info(f"Deleted {count} compute pools for product {product_name}")
        print(f"Deleted {count} compute pools for product {product_name}")
    else:
        logger.error("No product name provided, will not delete any compute pool")
        raise Exception("No product name provided, will not delete any compute pool")

# ------ Private methods ------

def _save_compute_pool_list(compute_pool_list: ComputePoolList):
    with open(COMPUTE_POOL_LIST_FILE, "w") as f:
        f.write(compute_pool_list.model_dump_json(indent=2, warnings=False))


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
    Wait for the compute pool to be provisionned
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
