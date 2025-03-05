import unittest
import sys
import os
# Order of the following code is important to make the tests working
os.environ["CONFIG_FILE"] = "./config.yaml"
module_path = "./utils"
sys.path.append(os.path.abspath(module_path))


class TestImportThing(unittest.TestCase):

    def test_first_stuff(self):
        """
        Intent of the test
        """
        print("\n\n >>> test_first_stuff\n")
        pass


if __name__ == '__main__':
    unittest.main()
    