import unittest, sys, os

module_path = "./utils"
sys.path.append(os.path.abspath(module_path))

from process_src_tables import find_table_already_processed

class Test_common_functions(unittest.TestCase):

    def test_table_present_in_table_to_proceed(self):
        print("test_table_present_in_table_to_proceed")
        assert find_table_already_processed("training_course", "./utils/reports/tables_to_process.txt")
        assert not find_table_already_processed("zouzou", "./utils/reports/tables_to_process.txt")

if __name__ == '__main__':
    unittest.main()