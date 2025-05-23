import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
#os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
#os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["CONFIG_FILE"]= "/Users/jerome/.shift_left/config-stage-flink.yaml"
os.environ["PIPELINES"]= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        
from shift_left.core.utils.app_config import get_config, shift_left_dir
from shift_left.core.models.flink_statement_model import ( 
    Statement, 
    StatementInfo, 
    StatementListCache, 
    Spec, 
    Status,
    FlinkStatementNode
)
import  shift_left.core.pipeline_mgr as pipeline_mgr

import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.test_mgr as test_mgr
import shift_left.core.table_mgr as table_mgr
from shift_left.core.utils.file_search import build_inventory
import json
class TestDebugUnitTests(unittest.TestCase):
        

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
    
    def test_build_ancestor_sorted_graph(self):
        node_map = {}
        node_map["src_x"] = FlinkStatementNode(table_name="src_x")
        node_map["src_y"] = FlinkStatementNode(table_name="src_y")
        node_map["src_b"] = FlinkStatementNode(table_name="src_b")
        node_map["src_a"] = FlinkStatementNode(table_name="src_a")
        node_map["x"] = FlinkStatementNode(table_name="x", parents=[node_map["src_x"]])
        node_map["y"] = FlinkStatementNode(table_name="y", parents=[node_map["src_y"]])
        node_map["b"] = FlinkStatementNode(table_name="b", parents=[node_map["src_b"]])
        node_map["z"] = FlinkStatementNode(table_name="z", parents=[node_map["x"], node_map["y"]])
        node_map["d"] = FlinkStatementNode(table_name="d", parents=[node_map["z"], node_map['y']])
        node_map["c"] = FlinkStatementNode(table_name="c", parents=[node_map["z"], node_map["b"]])
        node_map["p"] = FlinkStatementNode(table_name="p", parents=[node_map["z"]])
        node_map["a"] = FlinkStatementNode(table_name="a", parents=[node_map["src_x"], node_map["src_a"]])
   
        node_map["e"] = FlinkStatementNode(table_name="e", parents=[node_map["c"]])
        node_map["f"] = FlinkStatementNode(table_name="f", parents=[node_map["d"]])

        table_list = ['x','y','z','a','b']
        merged_nodes = {}
        merged_dependencies = []
        for node in table_list:
            nodes, dependencies = deployment_mgr._get_ancestor_subgraph(node_map[node], node_map)
            print(len(nodes) , len(dependencies))
            merged_nodes.update(nodes)
            for dep in dependencies:
                merged_dependencies.append(dep)
        print(len(merged_nodes))
        print(len(merged_dependencies))
        sorted_nodes = deployment_mgr._topological_sort(merged_nodes, merged_dependencies)
        for node in sorted_nodes:
            print(f"{node.table_name}")
        
       

if __name__ == '__main__':
    unittest.main()