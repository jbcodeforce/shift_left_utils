import os
import shutil
from pathlib import Path

import pytest

# Load before test modules: some imports call get_config() at module level.
# Mirror set_test_env / tests/it/conftest.py so pytest works without sourcing set_test_env.
_TESTS_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("SL_CONFIG_FILE", str(_TESTS_ROOT / "config.yaml"))
os.environ.setdefault(
    "PIPELINES",
    str(_TESTS_ROOT / "data" / "flink-project" / "pipelines"),
)
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


@pytest.fixture(autouse=True, scope="module")
def isolate_pipelines(tmp_path_factory):
    """Per-test isolation for filesystem artifacts used by core UTs.

    - Copy test pipelines into a unique temp dir
    - Point PIPELINES to that dir
    - Ensure SL_CONFIG_FILE is set
    - Reset caches and (re)build inventory and pipeline definitions
    """
    here = Path(__file__).resolve()
    # tests root: .../src/shift_left/tests
    tests_root = here.parents[2]
    source_pipelines = tests_root / "data" / "flink-project" / "pipelines"
    if not source_pipelines.exists():
        raise RuntimeError(f"Source test pipelines not found at {source_pipelines}")

    # Create an isolated copy under a module-scoped temp dir
    tmp_root = tmp_path_factory.mktemp("sl")
    tmp_pipelines = tmp_root / "pipelines"
    shutil.copytree(source_pipelines, tmp_pipelines)

    # Set environment for the code under test (manage env manually due to module scope)
    prev_pipelines = os.environ.get("PIPELINES")
    prev_config = os.environ.get("SL_CONFIG_FILE")
    os.environ["PIPELINES"] = str(tmp_pipelines)
    default_config = tests_root / "config.yaml"
    if default_config.exists():
        os.environ["SL_CONFIG_FILE"] = str(default_config)

    # Import after env is set so modules pick up correct settings
    from shift_left.core.utils.app_config import reset_all_caches
    from shift_left.core.utils.file_search import get_or_build_inventory
    import shift_left.core.pipeline_mgr as pm

    # Reset any module-level caches that could leak across tests
    reset_all_caches()

    # Clean and (re)build metadata for this isolated copy
    pm.delete_all_metada_files(str(tmp_pipelines))
    # Ensure inventory.json exists and is consistent
    get_or_build_inventory(str(tmp_pipelines), str(tmp_pipelines), recreate=True)
    # Build all pipeline_definition.json files for the tree
    pm.build_all_pipeline_definitions(str(tmp_pipelines))

    try:
        yield
    finally:
        # Restore environment
        if prev_pipelines is None:
            os.environ.pop("PIPELINES", None)
        else:
            os.environ["PIPELINES"] = prev_pipelines
        if prev_config is None:
            os.environ.pop("SL_CONFIG_FILE", None)
        else:
            os.environ["SL_CONFIG_FILE"] = prev_config

