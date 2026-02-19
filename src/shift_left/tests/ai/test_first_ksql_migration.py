import unittest
import os

"""
Copyright 2024-2026 Confluent, Inc.
"""

data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

from shift_left.ai.process_src_tables import migrate_one_file


class TestKsqlMigration(unittest.TestCase):

    def test_filtering_ksql_migration(self):
        staging = data_dir + "/flink-project/staging/ut/from_ksql"
        src_folder = data_dir + "/ksql-project"
        os.environ["STAGING"] = staging
        os.environ["SRC_FOLDER"] = src_folder
        os.environ["CONFIG_FILE"] = str(data_dir +  "../config-ccloud.yaml")
        sql_src = os.path.join(src_folder, "sources", "filtering.ksql")
        print("Starting KSQL migration (output below)...", flush=True)

        migrate_one_file(
            table_name="filtering",
            sql_src_file=sql_src,
            staging_target_folder=staging,
            source_type="ksql",
            product_name="orders",
            validate=False,
        )

        out_dir = os.path.join(staging, "orders", "filtering")
        scripts_dir = os.path.join(out_dir, "sql-scripts")
        assert os.path.exists(out_dir), f"Expected output dir: {out_dir}"
        assert os.path.exists(scripts_dir), f"Expected sql-scripts dir: {scripts_dir}"
        ddl_files = [f for f in os.listdir(scripts_dir) if f.startswith("ddl.") and f.endswith(".sql")]
        dml_files = [f for f in os.listdir(scripts_dir) if f.startswith("dml.") and f.endswith(".sql")]
        assert ddl_files, f"Expected at least one ddl.*.sql in {scripts_dir}"
        assert dml_files, f"Expected at least one dml.*.sql in {scripts_dir}"


if __name__ == "__main__":
    unittest.main()
