
"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for compute pool management functionality.
"""
from datetime import datetime
import unittest
import sys
import os
import pathlib
from typing import List, Dict, Any
from unittest.mock import patch, MagicMock
# Order of the following code is important to make the tests working
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
module_path = "./utils"
sys.path.append(os.path.abspath(module_path))

from shift_left.core.models.flink_compute_pool_model import (
    ComputePoolResponse,
    ComputePoolList,
    ComputePoolInfo,
    Metadata,
    Spec,
    Environment,
    Status,
    ComputePoolListResponse
)
import shift_left.core.compute_pool_mgr as cpm


class TestComputePoolMgr(unittest.TestCase):
    """Test suite for compute pool management functionality."""

    def _get_compute_pool_list(self, nb_items: int = 40) -> ComputePoolList:
        compute_pool_list = ComputePoolList()
        for idx in range(nb_items):
            cpi = ComputePoolInfo(
                    id=f"lfcp-ab{idx}",
                    name=f"dev-mv-config-{idx}",
                    env_id="env-xxx",
                    max_cfu=50,
                    region="us-west-2",
                    status_phase="PROVISIONED",
                    current_cfu=1
                )
            compute_pool_list.pools.append(cpi)         
        return compute_pool_list

    def _pool_info_to_pool_response(self, pool_info: ComputePoolInfo) -> ComputePoolResponse:
        return ComputePoolResponse(
            id=pool_info.id,
            spec={"display_name": pool_info.name, 
                "max_cfu": pool_info.max_cfu, 
                    "region": pool_info.region,
                "environment": {"id": pool_info.env_id, "resource_name": "crn://confluent.cloud/organization=5xxx6/environment=env-xxx"}},
            status={"current_cfu": pool_info.current_cfu, "phase": pool_info.status_phase},
            metadata={},
            api_version="fcpm/v2",
            kind="ComputePool"
        )

    def test_validate_compute_pool_response_model(self) -> None:
        """Test validation of compute pool response model."""
        response = """
{
    "api_version": "fcpm/v2",
    "id": "lfcp-xxxx",
    "kind": "ComputePool",
    "metadata": {
        "created_at": "2025-04-15T15:40:31.84352Z",
        "resource_name": "crn://confluent.cloud/organization=5xxx6/environment=env-xxx/flink-region=aws.us-west-2/compute-pool=lfcp-xxxx",
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
        compute_pool = ComputePoolResponse.model_validate_json(response)
        self.assertIsInstance(compute_pool, ComputePoolResponse)
        self.assertEqual(compute_pool.id, "lfcp-xxxx")
        self.assertEqual(compute_pool.spec.max_cfu, 50)
        self.assertEqual(compute_pool.status.current_cfu, 1)
        self.assertEqual(compute_pool.status.phase, "PROVISIONED")

    def test_compute_pool_list_model(self) -> None:
        """Test creation and validation of compute pool list model."""
        compute_pool_list = ComputePoolList()
        compute_pool_list.created_at = "2025-04-15T15:40:31.84352Z"
        compute_pool_list.pools = [
            ComputePoolInfo(
                id="lfcp-3gw3go",
                name="dev-mv-config",
                env_id="env-xxx",
                max_cfu=50,
                region="us-west-2",
                status_phase="PROVISIONED",
                current_cfu=1
            )
        ]
        self.assertEqual(len(compute_pool_list.pools), 1)
        self.assertEqual(compute_pool_list.pools[0].id, "lfcp-3gw3go")

    def test_compute_pool_response_model(self) -> None:
        """Test creation and validation of compute pool response model."""
        compute_pool = ComputePoolResponse()
        compute_pool.api_version = "fcpm/v2"
        compute_pool.id = "lfcp-3gw3go"
        compute_pool.kind = "ComputePool"
        compute_pool.metadata = Metadata(
            created_at="2025-04-15T15:40:31.84352Z",
            resource_name="crn://confluent.cloud/organization=5..96/environment=env-xxx/flink-region=aws.us-west-2/compute-pool=lfcp-xxxx",
            self="https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
            updated_at="2025-04-15T15:53:32.133555Z"
        )
        compute_pool.spec = Spec(
            cloud="AWS",
            display_name="dev-mv-config",
            enable_ai=False,
            environment=Environment(
                id="env-xxx",
                related="https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-xxxx",
                resource_name="crn://confluent.cloud/organization=5xxx-6/environment=env-xxx"
            ),
            max_cfu=50,
            region="us-west-2"
        )
        compute_pool.status = Status(
            current_cfu=1,
            phase="PROVISIONED"
        )
        self.assertEqual(compute_pool.id, "lfcp-3gw3go")
        self.assertEqual(compute_pool.spec.max_cfu, 50)
        self.assertEqual(compute_pool.status.phase, "PROVISIONED")

    def test_compute_pool_list_response_model(self) -> None:
        """Test validation of compute pool list response model."""
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
        compute_pool_list = ComputePoolListResponse.model_validate_json(response)
        self.assertIsNotNone(compute_pool_list.data)
        self.assertEqual(len(compute_pool_list.data), 2)
        self.assertEqual(compute_pool_list.data[0].id, "lfcp-xxxx")
        self.assertEqual(compute_pool_list.data[1].id, "lfcp-xvrvmz")

    @patch('shift_left.core.compute_pool_mgr.get_compute_pool_list')
    def test_search_for_matching_compute_pool(self, mock_get_compute_pool_list) -> None:
        """Test search functionality for matching compute pools."""
        compute_pool_list = self._get_compute_pool_list()
        mock_get_compute_pool_list.return_value = compute_pool_list
        self.assertEqual(len(compute_pool_list.pools), 40)
        
        matching_pools = cpm.search_for_matching_compute_pools("mv-config-3")
        self.assertIsNotNone(matching_pools)
        self.assertGreaterEqual(len(matching_pools), 1)            
        specific_pool = cpm.get_compute_pool_with_id(compute_pool_list, "lfcp-ab3")
        self.assertIsNotNone(specific_pool)
        self.assertEqual(specific_pool.id, "lfcp-ab3")
        self.assertEqual(specific_pool.name, "dev-mv-config-3")

    @patch('shift_left.core.compute_pool_mgr.ConfluentCloudClient')
    def test_get_compute_pool_list_cache(self, mock_confluent_cloud_client) -> None:
        """Test get compute pool list cache, then with expired cache"""

        mock_compute_pool_list = self._get_compute_pool_list(3)
        mock_confluent_cloud_client.return_value = MagicMock()
        list_a = [self._pool_info_to_pool_response(pool) for pool in mock_compute_pool_list.pools]
        response = ComputePoolListResponse(data=list_a, kind="ComputePoolList", metadata={})
        mock_confluent_cloud_client.return_value.get_compute_pool_list.return_value = response
        cpm.reset_compute_list()
        cpm._save_compute_pool_list(mock_compute_pool_list)
        compute_pool_list = cpm.get_compute_pool_list()
        self.assertEqual(len(compute_pool_list.pools), 3)
        # set the cache to be expired
        mock_compute_pool_list.created_at = datetime(2025,4,15,15,20,0)
        cpm._save_compute_pool_list(mock_compute_pool_list)
        cpm.reset_compute_list()    
        compute_pool_list = cpm.get_compute_pool_list()
        self.assertEqual(len(compute_pool_list.pools), 3)
        self.assertNotEqual(mock_compute_pool_list.created_at,compute_pool_list.created_at)


    @patch('shift_left.core.compute_pool_mgr.ConfluentCloudClient')
    def test_delete_compute_pool(self, mock_confluent_cloud_client) -> None:
        """Test delete compute pool."""
        mock_compute_pool_list = self._get_compute_pool_list(5)
        cpm._save_compute_pool_list(mock_compute_pool_list)

        mock_confluent_cloud_client.return_value = MagicMock()
        mock_confluent_cloud_client.return_value.delete_compute_pool.return_value = "Deleted"
        cpm.delete_compute_pool("lfcp-ab2")
        compute_pool_list = cpm.get_compute_pool_list()
        self.assertEqual(len(compute_pool_list.pools), 4)
        self.assertIsNone(cpm.get_compute_pool_with_id(compute_pool_list, "lfcp-ab2"))

    @patch('shift_left.core.compute_pool_mgr.ConfluentCloudClient')
    def test_create_compute_pool(self, mock_confluent_cloud_client) -> None:
        """Test create compute pool."""
        mock_compute_pool_list = self._get_compute_pool_list(4)
        cpm._save_compute_pool_list(mock_compute_pool_list)

        mock_confluent_cloud_client.return_value = MagicMock()
        result = {"id": "lfcp-ab8", "spec": {"display_name": "lfcp-ab8", "max_cfu": 50, "region": "us-west-2"}, "status": {"current_cfu": 0, "phase": "PROVISIONED"}}
        mock_confluent_cloud_client.return_value.create_compute_pool.return_value = result
        mock_confluent_cloud_client.return_value.get_compute_pool_info.return_value = result
        cpm.create_compute_pool("lfcp-ab8")
        compute_pool_list = cpm.get_compute_pool_list()
        self.assertEqual(len(compute_pool_list.pools), 5)
        self.assertIsNotNone(cpm.get_compute_pool_with_id(compute_pool_list, "lfcp-ab8"))

    @patch('shift_left.core.compute_pool_mgr.ConfluentCloudClient')
    def test_delete_compute_pool_for_product(self, mock_confluent_cloud_client) -> None:
        """Test delete compute pool for product."""
        cpm.reset_compute_list()
        mock_compute_pool_list = self._get_compute_pool_list(5)
        cpm._save_compute_pool_list(mock_compute_pool_list)

        mock_confluent_cloud_client.return_value = MagicMock()
        mock_confluent_cloud_client.return_value.delete_compute_pool.return_value = "Deleted"
        cpm.delete_all_compute_pools_of_product("mv")
        compute_pool_list = cpm.get_compute_pool_list()
        self.assertEqual(len(compute_pool_list.pools), 0)
       


if __name__ == '__main__':
    unittest.main()