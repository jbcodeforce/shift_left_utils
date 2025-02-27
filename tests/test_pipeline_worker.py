import unittest
import sys
import os
# Order of the following code is important to make the tests working
os.environ["CONFIG_FILE"] = "./config.yaml"
module_path = "./utils"
sys.path.append(os.path.abspath(module_path))
import json

from pipeline_helper import build_all_file_inventory, build_pipeline_definition_from_table, PIPELINE_JSON_FILE_NAME
from pipeline_worker import delete_metada_file, walk_the_hierarchy_for_report

class TestPipelineWorker(unittest.TestCase):

    def _test_delete_existing_metadata(self):
        source_folder="./examples"
        delete_metada_file(source_folder, PIPELINE_JSON_FILE_NAME)
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                if PIPELINE_JSON_FILE_NAME == file:
                    unittest.fail(f"A file {file} was found")

    def _test_create_pipelines_from_fact(self):
        """
        Validate the sink to source pipeline_metadata construction
        """
        all_files=build_all_file_inventory("./examples")
        hierarchy=build_pipeline_definition_from_table("./examples/facts/p1/fct_order/sql-scripts/dml.fct_order.sql", [], all_files)
        assert hierarchy
        assert len(hierarchy.children) == 0
        assert len(hierarchy.parents) == 2
        assert hierarchy.type == "fact"
        print(hierarchy.model_dump_json(indent=3))

    def test_walk(self):
        report = walk_the_hierarchy_for_report("./examples/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        print(json.dumps(report,indent=3))

    def test_walk_report(self):
        report=walk_the_hierarchy_for_report("../data-platform-flink/staging/../pipelines/dimensions/aqem/dim_tag/" + PIPELINE_JSON_FILE_NAME)
        print(json.dumps(report,indent=3))

    


if __name__ == '__main__':
    unittest.main()