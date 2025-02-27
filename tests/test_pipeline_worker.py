import unittest
import sys
import os
# Order of the following code is important to make the tests working
os.environ["CONFIG_FILE"] = "./config.yaml"
module_path = "./utils"
sys.path.append(os.path.abspath(module_path))
import json

from create_table_folder_structure import get_or_build_inventory_from_ddl
from pipeline_helper import build_pipeline_definition_from_table, PIPELINE_JSON_FILE_NAME
from pipeline_worker import delete_metada_file, walk_the_hierarchy_for_report

SRC_FOLDER="./examples/fink_project"

class TestPipelineWorker(unittest.TestCase):
    

    def test_delete_existing_metadata(self):
        delete_metada_file(SRC_FOLDER, PIPELINE_JSON_FILE_NAME)
        for root, dirs, files in os.walk(SRC_FOLDER):
            for file in files:
                if PIPELINE_JSON_FILE_NAME == file:
                    unittest.fail(f"A file {file} was found")

    def test_create_pipelines_from_fact(self):
        """
        Validate the sink to source pipeline_metadata construction
        """

        inventory=get_or_build_inventory_from_ddl(SRC_FOLDER + "/pipelines", SRC_FOLDER+"../", True)
        hierarchy=build_pipeline_definition_from_table(SRC_FOLDER + "/pipelines/facts/p1/fct_order/sql-scripts/dml.fct_order.sql", [], inventory)
        assert hierarchy
        assert len(hierarchy.children) == 0
        assert len(hierarchy.parents) == 2
        assert hierarchy.type == "fact"
        print(hierarchy.model_dump_json(indent=3))

    def test_walk(self):
        report = walk_the_hierarchy_for_report(SRC_FOLDER + "/pipelines/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        print(json.dumps(report,indent=3))



if __name__ == '__main__':
    unittest.main()