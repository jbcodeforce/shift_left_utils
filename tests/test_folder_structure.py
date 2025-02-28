import unittest
import sys
import os
# Order of the following code is important to make the tests working

module_path = "../utils"
sys.path.append(os.path.abspath(module_path))
from create_table_folder_structure import get_or_build_inventory_from_ddl, from_absolute_to_pipeline


class Test_folder_structure_functions(unittest.TestCase):

   def test_create_inventory(self):
       inventory=get_or_build_inventory_from_ddl(os.getenv("PIPELINES"),"./tmp", True)
       assert inventory
       assert len(inventory) > 2
       print(inventory)

   def test_load_inventory(self):
       inventory=get_or_build_inventory_from_ddl(os.getenv("PIPELINES"),"./tmp", False)
       assert inventory
       assert len(inventory) > 2
       for item in inventory:
            print(f"key : {item}")
            print(f"value: {inventory[item]} {type(inventory[item])}")

   def test_path_transformation(self):
      path = "/user/jerome/project/pipelines/dataproduct/sources/sql-scripts/ddl.table.sql"
      assert "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql" == from_absolute_to_pipeline(path)
      path = "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql"
      assert "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql" == from_absolute_to_pipeline(path)

    

if __name__ == '__main__':
    unittest.main()
    