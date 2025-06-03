"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil
import shift_left.core.project_manager as pm
from shift_left.core.utils.app_config import get_config

class TestProjectManager(unittest.TestCase):
    data_dir = ""

    @classmethod
    def setUpClass(cls):
        cls.data_dir = str(Path(__file__).parent / "../tmp")  # Path to the tmp directory


    def test_list_topic(self):
        topics = pm.get_topic_list("./topics.txt")
        assert topics
        assert os.path.exists("./topics.txt")
        with open("./topics.txt", "r") as f:
            lines = f.readlines()
            line_count = len(lines)
            assert line_count > 0
        os.remove("./topics.txt")

        
if __name__ == '__main__':
    unittest.main()