import unittest
import os

"""
Copyright 2024-2026 Confluent, Inc.
"""

data_dir=os.path.join(os.path.dirname(__file__),'..','data')

from shift_left.ai.process_src_tables import migrate_one_file

class TestPySparkMigration(unittest.TestCase):


    def test_raw_active_users_spark_migration(self):
        staging = data_dir + "/flink-project/staging"
        src_folder = data_dir + "/spark-project"
        os.environ["STAGING"] = staging
        os.environ["SRC_FOLDER"] = src_folder
        print("Starting PySpark migration...")

        migrate_one_file(
            table_name="raw_active_users",
            sql_src_file=src_file,
            staging_target_folder=staging,
            source_type="spark",
            product_name="users",
            validate=False,
        )


if __name__ == '__main__':
    unittest.main()
