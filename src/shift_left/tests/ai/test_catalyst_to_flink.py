"""
Copyright 2024-2026 Confluent, Inc.

Tests for Catalyst plan to Flink SQL visitor (FlinkSQLGenerator).
Uses fixture JSON from pyspark-project so tests run without Spark.
"""
import json
import os
import unittest

from shift_left.ai.catalyst_to_flink import FlinkSQLGenerator, catalyst_plan_to_flink_sql


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
FIXTURE_PLAN = os.path.join(DATA_DIR, "pyspark-project", "catalyst_plan.json")


class TestFlinkSQLGenerator(unittest.TestCase):
    def test_fixture_plan_generates_expected_flink_sql(self):
        """Visitor on fixture catalyst_plan.json produces the expected Flink SQL."""
        if not os.path.exists(FIXTURE_PLAN):
            self.skipTest(f"Fixture not found: {FIXTURE_PLAN}")
        with open(FIXTURE_PLAN) as f:
            plan = json.load(f)
        gen = FlinkSQLGenerator()
        gen.visit(plan)
        sql = gen.generate_sql()
        self.assertIn("SELECT user_id, COUNT(1) AS purchase_count", sql)
        self.assertIn("FROM default.ecommerce_events", sql)
        self.assertIn("WHERE event_name = 'purchase'", sql)
        self.assertIn("GROUP BY user_id", sql)
        self.assertTrue(sql.strip().endswith(";"))

    def test_catalyst_plan_to_flink_sql_convenience(self):
        """catalyst_plan_to_flink_sql() accepts string or dict and returns SQL."""
        if not os.path.exists(FIXTURE_PLAN):
            self.skipTest(f"Fixture not found: {FIXTURE_PLAN}")
        with open(FIXTURE_PLAN) as f:
            plan_str = f.read()
        sql = catalyst_plan_to_flink_sql(plan_str)
        self.assertIn("SELECT ", sql)
        self.assertIn("FROM ", sql)
        plan = json.loads(plan_str)
        sql2 = catalyst_plan_to_flink_sql(plan)
        self.assertEqual(sql, sql2)

    def test_sql_parts_filled_from_fixture(self):
        """Visitor fills select, from, where, group_by from fixture plan."""
        if not os.path.exists(FIXTURE_PLAN):
            self.skipTest(f"Fixture not found: {FIXTURE_PLAN}")
        with open(FIXTURE_PLAN) as f:
            plan = json.load(f)
        gen = FlinkSQLGenerator()
        gen.visit(plan)
        self.assertEqual(gen.sql_parts["from"], "default.ecommerce_events")
        self.assertIn("user_id", gen.sql_parts["select"])
        self.assertIn("COUNT(1) AS purchase_count", gen.sql_parts["select"])
        self.assertEqual(gen.sql_parts["group_by"], ["user_id"])
        self.assertEqual(gen.sql_parts["where"], ["event_name = 'purchase'"])


if __name__ == "__main__":
    unittest.main()
