"""
Base class for all unit tests
"""
from datetime import datetime
import unittest
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
from shift_left.core.models.flink_statement_model import (
    Statement,
    StatementInfo,
    Status,
    Spec,
    Metadata
)
from shift_left.core.utils.app_config import get_config, reset_all_caches
from shift_left.core.deployment_mgr import (
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)
import os

class BaseUT(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.TEST_COMPUTE_POOL_ID_1 = "test-pool-121"
        self.TEST_COMPUTE_POOL_ID_2 = "test-pool-122"
        self.TEST_COMPUTE_POOL_ID_3 = "test-pool-123"
        self.inventory_path = os.getenv("PIPELINES")

    def setUp(self):
        """
        Set up the test environment
        """
        # Reset all caches to ensure test isolation
        reset_all_caches()

    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_get_statement_info(
        self,
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = "test-pool-123"
    ) -> StatementInfo:
        """Create a mock StatementInfo object."""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )

    def _create_mock_statement(
        self,
        name: str = "statement_name",
        status_phase: str = "UNKNOWN"
    ) -> Statement:
        """Create a mock Statement object."""
        config=get_config()
        if config and config.get('flink'):
            properties={"sql.current-catalog":  config['flink']['catalog_name'],
                        "sql.current-database":  config['flink']['database_name']}
        else:
            properties={"sql.current-catalog": "default",
                        "sql.current-database": "default"}
        spec = Spec(compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, principal="test-principal", statement=name, properties=properties, stopped=False)
        metadata = Metadata(created_at=datetime.now().isoformat(),  resource_version="1",
                self="https://test-url",
                uid="test-uid")
        status = Status(phase=status_phase,detail="test-detail")
        return Statement(name=name, status=status, environment_id="test-env-123", spec=spec, metadata=metadata)


    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_1,
            name="test-pool",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_2 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_2,
            name="test-pool-2",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_3 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_3,
            name="dev-p1-fct-order",
            env_id=env_id,
            max_cfu=10,
            current_cfu=0
        )
        return ComputePoolList(pools=[pool_1, pool_2, pool_3])

    def _mock_assign_compute_pool(self, node: FlinkStatementNode,
                compute_pool_id: str,
                pool_creation: bool) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node. deployment_mgr._assign_compute_pool_id_to_node()"""

        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node

    def _create_mock_statement_node(
        self,
        table_name: str,
        product_name: str = "product1",
        dml_statement_name: str = "dml1",
        ddl_statement_name: str = "ddl1",
        compute_pool_id: str = ""
    ) -> FlinkStatementNode:
        """Create a mock FlinkStatementNode object."""
        node = FlinkStatementNode(
            table_name=table_name,
            product_name=product_name,
            dml_statement_name=dml_statement_name,
            ddl_statement_name=ddl_statement_name,
            compute_pool_id=compute_pool_id
        )
        # Initialize with default statement info if not set
        if not hasattr(node, 'existing_statement_info') or node.existing_statement_info is None:
            node.existing_statement_info = self._create_mock_get_statement_info(
                name=dml_statement_name,
                status_phase="UNKNOWN"
            )
        return node
