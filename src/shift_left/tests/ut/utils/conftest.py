import os
import shutil
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_pipelines(tmp_path, monkeypatch):
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

    # Create an isolated copy under tmp_path
    tmp_pipelines = tmp_path / "pipelines"
    shutil.copytree(source_pipelines, tmp_pipelines)

    # Set environment for the code under test
    monkeypatch.setenv("PIPELINES", str(tmp_pipelines))
    default_config = tests_root / "config.yaml"
    if default_config.exists():
        monkeypatch.setenv("CONFIG_FILE", str(default_config))

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

    yield

