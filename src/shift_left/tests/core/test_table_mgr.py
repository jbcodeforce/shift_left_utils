import unittest
import os
from pathlib import Path
import shift_left.core.table_mgr as tm

class TestTableManager(unittest.TestCase):

    def test_create_table_structure(self):
        try:
            tbf, tbn =tm.build_folder_structure_for_table("sink_table","./tmp/project_test/pipelines/facts")
            assert os.path.exists(tbf)
            assert os.path.exists(tbf + "/" + tm.SCRIPTS_FOLDER)
            assert os.path.exists(tbf + "/Makefile")
        except Exception as e:
            self.fail()
       


if __name__ == '__main__':
    unittest.main()