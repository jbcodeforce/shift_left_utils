
"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import sys
import os
import pathlib
# Order of the following code is important to make the tests working
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
module_path = "./utils"
sys.path.append(os.path.abspath(module_path))

from shift_left.core.flink_compute_pool_model import *
import shift_left.core.compute_pool_mgr as cpm


class TestComputePoolMgr(unittest.TestCase):

    def test_validate_compute_pool_response_model(self):
        response = """
{
    "api_version": "fcpm/v2",
    "id": "lfcp-xxxx",
    "kind": "ComputePool",
    "metadata": {
        "created_at": "2025-04-15T15: 40: 31.84352Z",
        "resource_name": "crn: //confluent.cloud/organization=5xxx6/environment=env-xxx/flink-region=aws.us-west-2/compute-pool=lfcp-xxxx",
        "self": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
        "updated_at": "2025-04-15T15:53:32.133555Z"
    },
    "spec": {
        "cloud": "AWS",
        "display_name": "dev-step-1",
        "enable_ai": false,
        "environment": {
            "id": "env-xxxx",
            "related": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
            "resource_name": "crn://confluent.cloud/organization=5xxx-6/environment=env-xxxx"
        },
        "max_cfu": 50,
        "region": "us-west-2"
    },
    "status": {
        "current_cfu": 1,
        "phase": "PROVISIONED"
    }
}
"""
        # Convert single quotes to double quotes for valid JSON
        compute_pool = ComputePoolResponse.model_validate_json(response)
        print(compute_pool)

    def test_compute_pool_list_model(self):
        cpl = ComputePoolList()
        cpl.created_at = "2025-04-15T15:40:31.84352Z"
        cpl.pools = [ComputePoolInfo(id="lfcp-3gw3go", 
                                     name="dev-mv-config", 
                                     env_id="env-xxx",
                                     max_cfu=50, 
                                     region="us-west-2", 
                                     status_phase="PROVISIONED", 
                                     current_cfu=1)]
        print(cpl)

    def test_compute_pool_response_model(self):
        cpl = ComputePoolResponse()
        cpl.api_version = "fcpm/v2"
        cpl.id = "lfcp-3gw3go"
        cpl.kind = "ComputePool"
        cpl.metadata = Metadata(created_at="2025-04-15T15:40:31.84352Z",
                                resource_name="crn://confluent.cloud/organization=5..96/environment=env-xxx/flink-region=aws.us-west-2/compute-pool=lfcp-xxxx",
                                self="https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
                                updated_at="2025-04-15T15:53:32.133555Z")
        cpl.spec = Spec(cloud="AWS",
                        display_name="dev-mv-config",
                        enable_ai=False,
                        environment=Environment(id="env-xxx",
                                                related="https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
                                                resource_name="crn://confluent.cloud/organization=5xxx-6/environment=env-xxx"),
                        max_cfu=50,
                        region="us-west-2")
        cpl.status = Status(current_cfu=1,
                            phase="PROVISIONED")
        print(cpl.model_dump_json(indent=4))

    def test_compute_pool_list_response_model(self):
        response = """{
            "api_version": "fcpm/v2",
            "data": [
                {
                    "api_version": "fcpm/v2", 
                    "id": "lfcp-xxxx",
                    "kind": "ComputePool",
                    "metadata": {
                        "created_at": "2025-03-21T01:25:48.363002Z",
                        "resource_name": "crn://confluent.cloud/organization=REDACTED/environment=env-xxxx/flink-region=aws.us-west-2/compute-pool=lfcp-xxxx",
                        "self": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
                        "updated_at": "2025-03-21T01:25:48.72268Z"
                    },
                    "spec": {
                        "cloud": "AWS",
                        "display_name": "cp-fct-order",
                        "enable_ai": false,
                        "environment": {
                            "id": "env-xxxx",
                            "related": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
                            "resource_name": "crn://confluent.cloud/organization=REDACTED/environment=env-xxxx"
                        },
                        "max_cfu": 10,
                        "region": "us-west-2"
                    },
                    "status": {
                        "current_cfu": 0,
                        "phase": "PROVISIONED"
                    }
                },
                {
                    "api_version": "fcpm/v2",
                    "id": "lfcp-xvrvmz", 
                    "kind": "ComputePool",
                    "metadata": {
                        "created_at": "2025-02-01T00:38:35.534374Z",
                        "resource_name": "crn://confluent.cloud/organization=REDACTED/environment=env-xxxx/flink-region=aws.us-west-2/compute-pool=lfcp-xvrvmz",
                        "self": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xvrvmz",
                        "updated_at": "2025-03-28T19:11:08.273122Z"
                    },
                    "spec": {
                        "cloud": "AWS",
                        "display_name": "j9r-pool",
                        "enable_ai": false,
                        "environment": {
                            "id": "env-xxxx",
                            "related": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xvrvmz",
                            "resource_name": "crn://confluent.cloud/organization=REDACTED/environment=env-xxxx"
                        },
                        "max_cfu": 30,
                        "region": "us-west-2"
                    },
                    "status": {
                        "current_cfu": 0,
                        "phase": "PROVISIONED"
                    }
                }
            ],
            "kind": "ComputePoolList",
            "metadata": {
                "first": "",
                "next": ""
            }
            }
            """
        cpl = ComputePoolListResponse.model_validate_json(response)
        print(cpl.model_dump_json(indent=4))
        assert cpl.data is not None
        assert len(cpl.data) == 2
        assert cpl.data[0].id == "lfcp-xxxx"
        assert cpl.data[1].id == "lfcp-xvrvmz"

    def test_search_for_matching_compute_pool(self):
        cpl = ComputePoolList()
        for idx in range(0,40):
            cpl.pools.append(ComputePoolInfo(id=f"lfcp-ab{idx}", 
                                                    name=f"dev-mv-config-{idx}", 
                                                    env_id="env-xxx",
                                                    max_cfu=50, 
                                                    region="us-west-2", 
                                                    status_phase="PROVISIONED", 
                                                    current_cfu=1))
        assert len(cpl.pools) == 40
        cpl2= cpm.search_for_matching_compute_pools(cpl, "mv-config-3")
        assert cpl2 is not None
        assert len(cpl2) >= 11
        for pool in cpl2:
            assert "dev-mv-config" in pool.name
        pool = cpm.get_compute_pool_with_id(cpl, "lfcp-ab3")
        assert pool is not None
        assert pool.id == "lfcp-ab3"
        assert pool.name == "dev-mv-config-3"


if __name__ == '__main__':
    unittest.main()