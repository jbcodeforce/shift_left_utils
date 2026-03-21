import unittest
import os

"""
Copyright 2024-2026 Confluent, Inc.
"""

data_dir=os.path.join(os.path.dirname(__file__),'..','data')

from shift_left.ai.process_src_tables import migrate_one_file

class TestSparkMigration(unittest.TestCase):


    def test_raw_active_users_spark_migration(self):
        staging = data_dir + "/flink-project/staging"
        src_folder = data_dir + "/spark-project"
        os.environ["STAGING"] = staging
        os.environ["SRC_FOLDER"] = src_folder
        print("Starting Spark migration...")
        sql_src_file = os.path.join(src_folder, "sources", "users", "raw_active_users.sql")
        print(f"Starting Spark migration for {sql_src_file}...", flush=True)
        migrate_one_file(
            table_name="raw_active_users",
            sql_src_file=sql_src_file,
            staging_target_folder=staging,
            source_type="spark",
            product_name="users",
            validate=True,
        )
        out_dir = os.path.join(staging, "users", "raw_active_users")
        scripts_dir = os.path.join(out_dir, "sql-scripts")
        assert os.path.exists(out_dir), f"Expected output dir: {out_dir}"
        assert os.path.exists(scripts_dir), f"Expected sql-scripts dir: {scripts_dir}"
        ddl_files = [f for f in os.listdir(scripts_dir) if f.startswith("ddl.") and f.endswith(".sql")]
        dml_files = [f for f in os.listdir(scripts_dir) if f.startswith("dml.") and f.endswith(".sql")]
        assert ddl_files, f"Expected at least one ddl.*.sql in {scripts_dir}"
        assert dml_files, f"Expected at least one dml.*.sql in {scripts_dir}"

if __name__ == '__main__':
    unittest.main()
