import os
import shutil
from pathlib import Path

import pytest


@pytest.fixture(autouse=True, scope="module")
def isolate_pipelines(tmp_path_factory):
    """Per-test isolation for filesystem artifacts used by core UTs.

    - Copy test pipelines into a unique temp dir
    - Point PIPELINES to that dir
    - Ensure CONFIG_FILE is set
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
    prev_config = os.environ.get("CONFIG_FILE")
    os.environ["PIPELINES"] = str(tmp_pipelines)
    default_config = tests_root / "config.yaml"
    if default_config.exists():
        os.environ["CONFIG_FILE"] = str(default_config)

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
            os.environ.pop("CONFIG_FILE", None)
        else:
            os.environ["CONFIG_FILE"] = prev_config

