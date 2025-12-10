"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
from importlib import import_module
import os
from typing import Tuple
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
from shift_left.core.utils.table_worker import ReplaceVersionInSqlContent

class TestBgTableWorker(unittest.TestCase):
    """
    Test the Blue Green version management using the Table Worker.
    When the version is set, the SQL with CREATE table needs to change the table name with the version as postfix.
    But when the table has already a version, the code needs to increase the version by 1.
    """


    def test_create_table_with_v2(self):
        sql_content = """
        CREATE TABLE src_table (
            id INT,
            name STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'localhost:9092'
        );
        """
        transformer = ReplaceVersionInSqlContent()
        updated, sql_out = transformer.update_sql_content(sql_content=sql_content, string_to_change_from="src_table", string_to_change_to="src_table_v2")
        assert updated
        assert "src_table_v2" in sql_out
        print(sql_out)

    def test_update_insert_statement_with_next_version(self):
        sql_content = """
        INSERT INTO dim_table (id, name, category)
        WITH cte1 as (
            SELECT id, name FROM src_table
        )
        SELECT cte1.id, cte1.name, c.category
        FROM cte1
        LEFT JOIN categories c ON cte1.id = c.id
        """
        transformer = ReplaceVersionInSqlContent()
        updated, sql_out = transformer.update_sql_content(sql_content=sql_content, string_to_change_from="dim_table", string_to_change_to="dim_table_v2")
        updated, sql_out = transformer.update_sql_content(sql_content=sql_out, string_to_change_from="src_table", string_to_change_to="src_table_v2")

        assert updated
        assert "dim_table_v2" in sql_out
        assert "src_table_v2" in sql_out
        print(sql_out)


if __name__ == "__main__":
    unittest.main()
