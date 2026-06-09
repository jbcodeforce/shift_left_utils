"""
Copyright 2024-2025 Confluent, Inc.
"""
import time
import os, json
from datetime import datetime
from importlib import import_module
from typing import Tuple, List, Optional
from shift_left.core.utils.app_config import get_config, logger, session_log_dir
from shift_left.core.models.flink_compute_pool_model import ComputePoolList, ComputePoolInfo, ComputePoolListResponse, ComputePoolResponse
from shift_left.core.utils.naming_convention import ComputePoolNameModifier
from shift_left.core.utils.ccloud_client import ConfluentCloudClient


STATEMENT_COMPUTE_POOL_FILE=session_log_dir + "/pool_assignments.json"
COMPUTE_POOL_LIST_FILE=session_log_dir + "/compute_pool_list.json"
COMPUTE_POOL_URL = "https://api.confluent.cloud/fcpm/v2/compute-pools"

_compute_pool_list = None
def get_compute_pool_list(env_id: str = None, region: str = None) -> ComputePoolList:
    global _compute_pool_list
    config = get_config()
    if not env_id:
        env_id = config['confluent_cloud']['environment_id']
    if not region:
        region = config['confluent_cloud']['cloud_region']
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
            _compute_pool_list = ComputePoolList(created_at=datetime.now())
            client = ConfluentCloudClient(get_config())
            page_size = config["confluent_cloud"].get("page_size", 100)
            auth_header = client.get_ccloud_auth()
            url=f"{COMPUTE_POOL_URL}?spec.region={region}&environment={env_id}&page_size={page_size}"
            next_page_token = previous_token = None
            while True:
                if next_page_token:
                    resp=client.make_request(method="GET", url=next_page_token+"&page_size="+str(page_size), auth_header=auth_header)
                    resp_obj = ComputePoolListResponse.model_validate(resp)
                else:
                    resp=client.make_request(method="GET", url=url, auth_header=auth_header)
                    response = ComputePoolListResponse.model_validate(resp)
                    resp_obj = response
                logger.debug(f"compute pool response as dict= {resp}")
                try:
                    if resp_obj.data:
                        for pool in resp_obj.data:
                            _compute_pool_list.pools.append(ComputePoolInfo(
                                id=pool.id,
                                name=pool.spec.display_name,
                                env_id=pool.spec.environment.id,
                                max_cfu=pool.spec.max_cfu,
                                region=pool.spec.region,
                                status_phase=pool.status.phase,
                                current_cfu=pool.status.current_cfu,
                                cloud=pool.spec.cloud
                                ))

                    if resp_obj.metadata.next:
                        next_page_token = resp_obj.metadata.next
                        if not next_page_token or next_page_token == previous_token:
                            break
                        previous_token = next_page_token
                    else:
                        break
                except Exception as e:
                    logger.error(f"Error parsing compute pool response: {e}")
                    logger.error(f"Response: {resp}")
                    break
            _save_compute_pool_list(_compute_pool_list)
            logger.info(f"Compute pool list has {len(_compute_pool_list.pools)} compute pools")
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Compute pool list has {len(_compute_pool_list.pools)} compute pools")
            return _compute_pool_list
    elif (_compute_pool_list.created_at
         and (datetime.now() - _compute_pool_list.created_at).total_seconds() > get_config()['app']['cache_ttl']):
        logger.info("Compute pool list cache is expired, reload it")
        reset_compute_list()
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
    if compute_pool_id:
        for pool in compute_pool_list.pools:
            if pool.id == compute_pool_id:
                return pool
        return get_compute_pool(compute_pool_id)
    return None

def get_compute_pool(id: str) -> ComputePoolInfo:
    client = ConfluentCloudClient(get_config())
    auth_header = client.get_ccloud_auth()
    env_id = get_config()['confluent_cloud']['environment_id']
    url=f"{COMPUTE_POOL_URL}/{id}?environment={env_id}"
    resp=client.make_request(method="GET", url=url, auth_header=auth_header)
    if resp.get('errors'):
        logger.error(f"Error getting compute pool {id}: {resp.get('errors')}")
        return None
    logger.info(f"compute pool info response as dict= {resp}")
    resp_obj= ComputePoolResponse.model_validate(resp)
    return ComputePoolInfo(
        id=resp_obj.id,
        name=resp_obj.spec.display_name,
        env_id=resp_obj.spec.environment.id,
        max_cfu=resp_obj.spec.max_cfu,
        region=resp_obj.spec.region,
        cloud=resp_obj.spec.cloud,
        status_phase=resp_obj.status.phase,
        current_cfu=resp_obj.status.current_cfu
    )


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

    compute_pool_list = get_compute_pool_list()
    pool = get_compute_pool_with_id(compute_pool_list,compute_pool_id)
    if pool:
        ratio = get_pool_usage_from_pool_info(pool)
        logger.info(f"Validate the {pool} exists and uses {ratio} % resources")
        if ratio >= config['flink'].get('max_cfu_percent_before_allocation', .7):
            logger.error(f"The CFU usage at {ratio} % is too high for {compute_pool_id}")
            return False
        return  pool.status_phase == "PROVISIONED"
    else:
        return False



def create_compute_pool(table_name: str) -> Tuple[str, str]:
    config = get_config()
    spec = _build_compute_pool_spec(table_name, config)
    logger.info(f"Create compute pool {spec['display_name']} for {table_name} ... it may take a while")
    print(f"Create compute pool {spec} for {table_name} ... it may take a while")
    try:
        client = ConfluentCloudClient(get_config());
        auth_header = client.get_ccloud_auth()
        data={'spec': spec}
        url=f"{COMPUTE_POOL_URL}"
        result=client.make_request(method="POST", url=url, auth_header=auth_header, data=data)
        if result and not result.get('errors'):
            pool: ComputePoolResponse = ComputePoolResponse.model_validate(result)
            if _verify_compute_pool_provisioned(pool.id):
                logger.info(f"Compute pool {pool.id} created and provisioned")
                get_compute_pool_list().pools.append(ComputePoolInfo(
                    id=pool.id,
                    name=pool.spec.display_name,
                    env_id=pool.spec.environment.id,
                    max_cfu=pool.spec.max_cfu,
                    region=pool.spec.region,
                    cloud=pool.spec.cloud,
                    status_phase=pool.status.phase,
                    current_cfu=pool.status.current_cfu
                ))
                return pool.id, pool.spec.display_name
            else:
                logger.error(f"Compute pool {pool.id} not provisioned")
                print(f"Compute pool {pool.id} not provisioned")
                return pool.id, pool.spec.display_name
        elif result.get('errors')[0].get('status') == "409":
            logger.error(f"Compute pool {spec['display_name']} already exists")
            print(f"Compute pool {spec['display_name']} already exists")
            return "", spec['display_name']
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
    auth_header = client.get_ccloud_auth()
    url=f"{COMPUTE_POOL_URL}/{compute_pool_id}?environment={env_id}"
    client.make_request(method="DELETE", url=url, auth_header=auth_header)
    _cp_list = get_compute_pool_list()
    _cp_list.pools.remove(get_compute_pool_with_id(_cp_list, compute_pool_id))
    _save_compute_pool_list(_cp_list)


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


def delete_compute_pools_by_ids(pool_ids: List[str], product_name: Optional[str] = None) -> int:
    """
    Delete compute pools listed by ID. product_name is used for logging only.
    Unknown IDs are skipped with a warning.
    """
    if not pool_ids:
        logger.error("No compute pool IDs provided, will not delete any compute pool")
        raise Exception("No compute pool IDs provided, will not delete any compute pool")

    compute_pool_list = get_compute_pool_list()
    log_context = f" for product {product_name}" if product_name else ""
    logger.info(f"Delete compute pools from list{log_context}: {pool_ids}")
    print(f"Delete compute pools from list{log_context}")

    count = 0
    for pool_id in pool_ids:
        pool = get_compute_pool_with_id(compute_pool_list, pool_id)
        if pool is None:
            print(f"[yellow]Warning: compute pool {pool_id} not found, skipping[/yellow]")
            logger.warning(f"Compute pool {pool_id} not found, skipping")
            continue
        delete_compute_pool(pool_id)
        print(f"Deleted compute pool {pool_id}")
        count += 1

    logger.info(f"Deleted {count} compute pools from list{log_context}")
    print(f"Deleted {count} compute pools from list{log_context}")
    return count

# ------ Private methods ------

def _save_compute_pool_list(compute_pool_list: ComputePoolList):
    with open(COMPUTE_POOL_LIST_FILE, "w") as f:
        f.write(compute_pool_list.model_dump_json(indent=2, warnings=False))


def _build_compute_pool_spec(table_name: str, config: dict) -> dict:
    spec = {}
    spec['display_name'] = _get_compute_pool_name_modifier().build_compute_pool_name_from_table(table_name)
    spec['cloud'] = config['confluent_cloud']['cloud_provider']
    spec['region'] = config['confluent_cloud']['cloud_region']
    spec['max_cfu'] =  config['flink']['max_cfu']
    if spec['max_cfu'] not in [5, 10, 20, 30, 40, 50]:
        spec['max_cfu'] = 10
    spec['environment'] = { 'id': config['confluent_cloud']['environment_id']}
    return spec

def _verify_compute_pool_provisioned(pool_id: str) -> bool:
    """
    Wait for the compute pool to be provisionned
    """
    provisioning = True
    failed = False
    while provisioning:
        logger.info("Wait ...")
        time.sleep(5)
        result= get_compute_pool(pool_id)
        provisioning = (result.status_phase == "PROVISIONING")
        failed = (result.status_phase == "FAILED")
    return not failed


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
