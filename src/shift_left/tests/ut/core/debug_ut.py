import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
import datetime
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
#os.environ["CONFIG_FILE"]= "/Users/jerome/.shift_left/config-stage-flink.yaml"
#os.environ["PIPELINES"]= "/Users/jerome/Code/customers/mc/data-platform-flink/pipelines"
        
from shift_left.core.utils.app_config import get_config, shift_left_dir
from shift_left.core.models.flink_statement_model import ( 
    Statement, 
    StatementInfo, 
    StatementListCache, 
    Spec, 
    Metadata,
    Status,
    FlinkStatementNode
)
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.deployment_mgr as dm
import shift_left.core.test_mgr as test_mgr
import shift_left.core.table_mgr as table_mgr
from shift_left.core.utils.file_search import build_inventory

class TestDebugUnitTests(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        cls.inventory_path = os.getenv("PIPELINES")
        pipeline_mgr.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def _test_explain_table(self):
        result = """== Physical Plan ==\n\nStreamSink [20]\n  +- StreamUnion [19]\n    +- StreamUnion [17]\n    :  +- StreamUnion [15]\n    :  :  +- StreamCalc [13]\n    :  :  :  +- StreamJoin [12]\n    :  :  :    +- StreamExchange [5]\n    :  :  :    :  +- StreamChangelogNormalize [4]\n    :  :  :    :    +- StreamExchange [3]\n    :  :  :    :      +- StreamCalc [2]\n    :  :  :    :        +- StreamTableSourceScan [1]\n    :  :  :    +- StreamExchange [11]\n    :  :  :      +- StreamCalc [10]\n    :  :  :        +- StreamChangelogNormalize [9]\n    :  :  :          +- StreamExchange [8]\n    :  :  :            +- StreamCalc [7]\n    :  :  :              +- StreamTableSourceScan [6]\n    :  :  +- StreamCalc [14]\n    :  :    +- (reused) [9]\n    :  +- StreamCalc [16]\n    :    +- (reused) [9]\n    +- StreamCalc [18]\n      +- (reused) [9]\n\n== Physical Details ==\n\n[1] StreamTableSourceScan\nTable: `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`src_identity_metadata`\nPrimary key: (id,tenant_id)\nChangelog mode: upsert\nUpsert key: (id,tenant_id)\nState size: low\nStartup mode: earliest-offset\n\n[2] StreamCalc\nChangelog mode: upsert\nUpsert key: (id,tenant_id)\n\n[3] StreamExchange\nChangelog mode: upsert\nUpsert key: (id,tenant_id)\n\n[4] StreamChangelogNormalize\nChangelog mode: retract\nUpsert key: (id,tenant_id)\nState size: medium\nState TTL: never\n\n[5] StreamExchange\nChangelog mode: retract\nUpsert key: (id,tenant_id)\n\n[6] StreamTableSourceScan\nTable: `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`stage_tenant_dimension`\nPrimary key: (tenant_id)\nChangelog mode: upsert\nUpsert key: (tenant_id)\nState size: low\nStartup mode: earliest-offset\n\n[7] StreamCalc\nChangelog mode: upsert\nUpsert key: (tenant_id)\n\n[8] StreamExchange\nChangelog mode: upsert\nUpsert key: (tenant_id)\n\n[9] StreamChangelogNormalize\nChangelog mode: retract\nUpsert key: (tenant_id)\nState size: medium\nState TTL: never\n\n[10] StreamCalc\nChangelog mode: retract\n\n[11] StreamExchange\nChangelog mode: retract\n\n[12] StreamJoin\nChangelog mode: retract\nState size: medium\nState TTL: never\n\n[13] StreamCalc\nChangelog mode: retract\n\n[14] StreamCalc\nChangelog mode: retract\n\n[15] StreamUnion\nChangelog mode: retract\n\n[16] StreamCalc\nChangelog mode: retract\n\n[17] StreamUnion\nChangelog mode: retract\n\n[18] StreamCalc\nChangelog mode: retract\n\n[19] StreamUnion\nChangelog mode: retract\n\n[20] StreamSink\nTable: `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`aqem_dim_role`\nPrimary key: (sid)\nChangelog mode: upsert\nState size: high\nState TTL: never\n\n== Warnings ==\n\n1. For StreamSink [20]: The primary key does not match the upsert key derived from the query. If the primary key and upsert key don't match, the system needs to add a state-intensive operation for correction. Please revisit the query (upsert key: null) or the table declaration for `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`aqem_dim_role` (primary key: [sid]). For more information, see https://cnfl.io/primary_vs_upsert_key.\n2. Entire statement: Your query includes one or more highly state-intensive operators but does not set a time-to-live (TTL) value, which means that the system potentially needs to store an infinite amount of state. This can result in a DEGRADED statement and higher CFU consumption. If possible, change your query to use a different operator, or set a time-to-live (TTL) value. For more information, see https://cnfl.io/high_state_intensive_operators.\n'"""
        print(result)

    def _test_explain_tables_for_product(self):
      table_mgr.explain_table(table_name="aqem_dim_role", compute_pool_id="lfcp-0725o5")
    
    
    def _test_statis_parsing_of_explain_table(self):
        explain_reports = []
        for root, dirs, files in os.walk(shift_left_dir):
            for file in files:
                if file.endswith("_explain_report.json"):
                    report_path = os.path.join(root, file)
                    print(f"Found explain report: {report_path}")
                    with open(report_path, 'r') as f:
                        explain_reports.append(json.load(f))
                    print(f"--> {table_mgr._summarize_trace(explain_reports[-1]['trace'])}")
    
    TEST_COMPUTE_POOL_ID_1 = "lfcp-121"
    def _create_mock_get_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
    ) -> StatementInfo:
        """Mock the call to get statement info"""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )
    
    def _mock_assign_compute_pool(self, node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node. deployment_mgr._assign_compute_pool_id_to_node()"""
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node
    
    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_1,
            name="test-pool-1",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        return ComputePoolList(pools=[pool_1])
    
    

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_execute_plan_in_parallel(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        restarting the leaf "f" and all parents. 
        """
        print("\n--> test_execute_plan_in_parallel, should runs src in parallel")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            return self._create_mock_get_statement_info(status_phase="UNKNOWN")
 
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list

        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="z", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=False, # should get same result if true
            force_ancestors=True,
            execute_plan=False
        )
        autonomous_nodes = dm._build_autonomous_nodes(execution_plan.nodes)
        print(f"autonomous_nodes: {autonomous_nodes}")
        nodes_to_execute = dm._get_nodes_to_execute(execution_plan.nodes)
        print(f"nodes_to_execute: {nodes_to_execute}")
        print(f"{summary}")
        assert len(execution_plan.nodes) == 5  # all nodes are present as we want to see running ones too
        assert len(nodes_to_execute) == 5
        assert len(autonomous_nodes) == 2
        for node in execution_plan.nodes:
            if node.table_name in ["src_x", "x", "src_y", "y"]:
                assert node.to_run is True
                assert node.to_restart is False



       

if __name__ == '__main__':
    unittest.main()