import unittest, sys, os

module_path = "./utils"
sys.path.append(os.path.abspath(module_path))

from process_src_tables import remove_already_processed_table

class Test_common_functions(unittest.TestCase):

    def test_table_present_in_table_to_proceed(self):
        print("test_table_present_in_table_to_proceed")
        assert remove_already_processed_table(["int_training_course_deduped"])
        assert not remove_already_processed_table(["zouzou"])

if __name__ == '__main__':
    unittest.main()