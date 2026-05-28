"""Pytest configuration for integration tests under tests/it/."""
import os
from pathlib import Path

import pytest

# Load before test modules: pipeline CLI (and other entry points) import code that
# calls get_config() at import time. Use tests/config.yaml plus dummy env vars
# so collection works without a real Confluent project (same idea as set_test_env).
_IT_DIR = Path(__file__).resolve().parent
_TESTS_ROOT = _IT_DIR.parent

_default_cfg = str(_TESTS_ROOT / "config.yaml")
os.environ.setdefault("SL_CONFIG_FILE", _default_cfg)
os.environ.setdefault("PIPELINES", str(_TESTS_ROOT / "data" / "flink-project" / "pipelines"))
if not os.environ.get("SL_CONFLUENT_CLOUD_API_KEY"):
    os.environ.setdefault("SL_KAFKA_API_KEY", "test")
    os.environ.setdefault("SL_KAFKA_API_SECRET", "test")
    os.environ.setdefault("SL_KAFKA_CLUSTER_ID", "lkc-test")
    os.environ.setdefault("SL_CONFLUENT_CLOUD_API_KEY", "test")
    os.environ.setdefault("SL_CONFLUENT_CLOUD_API_SECRET", "test")
    os.environ.setdefault("SL_FLINK_API_KEY", "test")
    os.environ.setdefault("SL_FLINK_API_SECRET", "test")
    os.environ.setdefault("SL_CLOUD_PROVIDER", "aws")
    os.environ.setdefault("SL_CLOUD_REGION", "us-west-2")
    os.environ.setdefault("SL_CLOUD_ORGANIZATION_ID", "id-org-test")
    os.environ.setdefault("SL_FLINK_ENV_ID", "env-nknqp3")
    os.environ.setdefault("SL_CONFLUENT_PRINCIPAL_ID", "sa-test")
    os.environ.setdefault("SL_FLINK_COMPUTE_POOL_ID", "lfcp-xvrvmz")
    os.environ.setdefault("SL_FLINK_ENV_NAME", "j9r-env")
    os.environ.setdefault("SL_FLINK_DATABASE_NAME", "j9r-kafka")


def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as integration tests."""
    for item in items:
        item.add_marker(pytest.mark.integration)
