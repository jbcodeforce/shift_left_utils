
"""
Copyright 2024-2025 Confluent, Inc.

Live Confluent Cloud integration tests for compute pool management.

These tests call the Confluent API. They are skipped unless you opt in and use
real credentials (dummy keys from tests/it/conftest.py return empty pool lists).

Run from ``src/shift_left`` after:

  source set_demo_env
  export SHIFT_LEFT_RUN_CLOUD_IT=1
  uv run pytest tests/it/test_compute_pool.py -v

Optional destructive test (deletes a compute pool referenced in the test):

  export SHIFT_LEFT_RUN_COMPUTE_POOL_DESTRUCTIVE=1
"""
import os
import pathlib
import unittest
from shift_left.core.utils.app_config import get_config
import shift_left.core.compute_pool_mgr as m

_TESTS_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONFIG_CCLOUD = _TESTS_ROOT / "config-ccloud.yaml"
_PIPELINES = _TESTS_ROOT / "data" / "flink-project" / "pipelines"


def _run_compute_pool_it() -> bool:
    """True when cloud IT is requested and API key is not the conftest placeholder."""
    cloud = os.environ.get("SHIFT_LEFT_RUN_CLOUD_IT", "").lower() in (
        "1",
        "true",
        "yes",
    )
    key = os.environ.get("SL_CONFLUENT_CLOUD_API_KEY", "")
    return bool(cloud and key not in ("", "test"))


def _run_destructive_compute_pool_it() -> bool:
    return os.environ.get("SHIFT_LEFT_RUN_COMPUTE_POOL_DESTRUCTIVE", "").lower() in (
        "1",
        "true",
        "yes",
    )


_RUN_IT = _run_compute_pool_it()
_RUN_DESTRUCTIVE = _run_destructive_compute_pool_it()

_SKIP_MSG = (
    "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent credentials "
    "(e.g. source set_demo_env from src/shift_left). "
    "conftest's SL_CONFLUENT_CLOUD_API_KEY=test yields empty API results."
)
_SKIP_DESTRUCTIVE = (
    "Set SHIFT_LEFT_RUN_COMPUTE_POOL_DESTRUCTIVE=1 to run the pool delete test"
)


@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestComputePoolMgr(unittest.TestCase):
    """Compute pool list/search against Confluent Cloud (opt-in)."""

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("CONFIG_FILE", str(_CONFIG_CCLOUD))
        os.environ.setdefault("SL_CONFIG_FILE", str(_CONFIG_CCLOUD))
        os.environ.setdefault("PIPELINES", str(_PIPELINES))
        m.reset_compute_list()
        cls._compute_mgr = m

    def test_1_compute_pool_list(self):
        config = get_config()
        print(
            f"test_1_compute_pool_list: environment "
            f"{config.get('confluent_cloud').get('environment_id')}"
        )
        cpl = self._compute_mgr.get_compute_pool_list(
            config.get("confluent_cloud").get("environment_id"),
            region=config.get("confluent_cloud").get("region"),
        )
        print(cpl.model_dump_json(indent=3))
        self.assertIsNotNone(cpl.pools)
        self.assertGreater(len(cpl.pools), 0, "No compute pools returned; check API credentials and environment")
        p0 = cpl.pools[0]
        self.assertEqual(
            p0.env_id, config.get("confluent_cloud").get("environment_id")
        )
        self.assertEqual(p0.region, config.get("confluent_cloud").get("region"))
        self.assertEqual(p0.status_phase, "PROVISIONED")
        self.assertGreaterEqual(p0.current_cfu, 0)
        self.assertGreater(p0.max_cfu, 0)

    def test_2_search_for_matching_compute_pools(self):
        config = get_config()
        print("test_2_search_for_matching_compute_pools: match convention for dev-j9r-pool")
        expected_name = self._compute_mgr._get_compute_pool_name_modifier().build_compute_pool_name_from_table(
            "p1-fct-order"
        )
        cpl = self._compute_mgr.search_for_matching_compute_pools("dev-j9r-pool")
        self.assertIsNotNone(cpl)
        self.assertGreater(
            len(cpl), 0, f"No pool named {expected_name!r}; create it or adjust config / table name"
        )
        self.assertEqual(cpl[0].name, expected_name)
        self.assertEqual(
            cpl[0].env_id, config.get("confluent_cloud").get("environment_id")
        )
        self.assertEqual(
            cpl[0].region, config.get("confluent_cloud").get("region")
        )
        self.assertEqual(cpl[0].status_phase, "PROVISIONED")
        self.assertGreaterEqual(cpl[0].current_cfu, 0)
        self.assertGreater(cpl[0].max_cfu, 0)

    def test_2_1_get_compute_pool_with_id(self):
        print("test_2_1_get_compute_pool_with_id")
        config = get_config()
        cpl = self._compute_mgr.get_compute_pool_list()
        pool_id = config.get("flink", {}).get("compute_pool_id")
        self.assertTrue(
            pool_id, "flink.compute_pool_id must be set in the active config (e.g. config-ccloud.yaml)"
        )
        pool = self._compute_mgr.get_compute_pool_with_id(cpl, pool_id)
        self.assertIsNotNone(
            pool, f"Compute pool id {pool_id!r} not in list for this environment"
        )
        self.assertEqual(pool.id, pool_id)

    def test_3_create_existing_compute_pool(self):
        print("test_3_create_existing_compute_pool: expect error if pool already exists")
        with self.assertRaises(Exception):
            self._compute_mgr.create_compute_pool("dev-j9r-pool")

    @unittest.skipUnless(_RUN_DESTRUCTIVE, _SKIP_DESTRUCTIVE)
    def test_4_test_pool_validation_and_delete(self):
        print("test_4: validate and delete p1-test-table pool (destructive)")
        pool_list = self._compute_mgr.search_for_matching_compute_pools(
            "p1-test-table"
        )
        self.assertGreater(
            len(pool_list),
            0,
            "No matching pool for p1-test-table; skip or create before running destructive test",
        )
        compute_pool_id = pool_list[0].id
        compute_pool_name = pool_list[0].name
        self.assertIsNotNone(compute_pool_id)
        self.assertIsNotNone(compute_pool_name)
        self.assertTrue(
            self._compute_mgr.is_pool_valid(compute_pool_id),
        )
        self._compute_mgr.delete_compute_pool(compute_pool_id)


if __name__ == "__main__":
    unittest.main()
