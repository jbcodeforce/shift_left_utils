import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
import re

# Load .env file BEFORE any imports that might need environment variables
def load_env_file(env_file_path):
    """Load environment variables from a .env file and return as dict."""
    env_vars = {}
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as f:
            lines = f.readlines()

        # First pass: load all variables without expansion
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                env_vars[key] = value

        # Second pass: expand variables (handle dependencies)
        max_iterations = 10  # Prevent infinite loops
        for _ in range(max_iterations):
            changed = False
            for key, value in env_vars.items():
                if '$' in value:
                    # Expand ${VAR} and $VAR patterns
                    def expand_var(match):
                        var_name = match.group(1) or match.group(2)
                        # First check env_vars, then os.environ
                        if var_name in env_vars:
                            return env_vars[var_name]
                        elif var_name in os.environ:
                            return os.environ[var_name]
                        return match.group(0)  # Return original if not found

                    new_value = re.sub(r'\$\{([^}]+)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)', expand_var, value)
                    if new_value != value:
                        env_vars[key] = new_value
                        changed = True
            if not changed:
                break

        # Set all variables in os.environ
        for key, value in env_vars.items():
            os.environ[key] = value

    return env_vars

# Try to find and load .env file
_env_file_paths = [
    # Path from launch.json (relative to workspaceFolder)
    pathlib.Path(__file__).parent.parent.parent.parent.parent.parent / "flink_project_demos" / "customer_360" / "c360_flink_processing" / ".env",
    # Alternative path
    pathlib.Path(__file__).parent.parent.parent.parent.parent / "flink_project_demos" / "customer_360" / "c360_flink_processing" / ".env",
]

_env_loaded = False
for env_file in _env_file_paths:
    if env_file.exists():
        load_env_file(str(env_file))
        print(f"✓ Loaded environment variables from: {env_file}")
        _env_loaded = True
        break

if not _env_loaded:
    print(f"⚠ .env file not found. Tried paths: {_env_file_paths}")

# Debug: Print key environment variables to verify they're loaded
if __name__ == '__main__' or True:  # Always print when debugging
    print("\n=== Environment Variables at Module Load ===")
    key_vars = ['FLINK_PROJECT', 'CONFIG_FILE', 'PIPELINES', 'SRC_FOLDER', 'STAGING']
    for var in key_vars:
        value = os.environ.get(var, 'NOT SET')
        print(f"{var}: {value}")
    print("=" * 50 + "\n")

#os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
#data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
#os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
#os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")

from shift_left.core.utils.app_config import get_config
import  shift_left.core.pipeline_mgr as pipeline_mgr
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.metric_mgr as metric_mgr
import shift_left.core.test_mgr as test_mgr
import shift_left.core.table_mgr as table_mgr
from typer.testing import CliRunner
from shift_left.cli import app

import shift_left.core.statement_mgr as sm
import shift_left.core.deployment_mgr as dm

def get_env_for_cli():
    """Get all environment variables that should be passed to CliRunner."""
    # Start with current environment
    env = dict(os.environ)
    return env

class TestDebugIntegrationTests(unittest.TestCase):

    def test_at_cli_level(self):
        runner = CliRunner()
        # Get environment variables to pass to CliRunner
        env = get_env_for_cli()

        # Verify key environment variables are set
        key_vars = ['FLINK_PROJECT', 'CONFIG_FILE', 'PIPELINES', 'SRC_FOLDER', 'STAGING']
        print("\n=== Environment Variables Check ===")
        for var in key_vars:
            value = env.get(var, 'NOT SET')
            print(f"{var}: {value}")
        print("=" * 40 + "\n")

        #result = runner.invoke(app, ['pipeline', 'deploy', '--table-name', 'aqem_fct_event_action_item_assignee_user', '--force-ancestors', '--cross-product-deployment'], env=env)
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'src_qx_training_trainee', '--may-start-descendants', '--cross-product-deployment'], env=env)
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--product-name', 'qx'], env=env)
        #result = runner.invoke(app, ['table', 'migrate', 'dim_training_course', os.getenv('SRC_FOLDER','.') + '/dimensions/qx/dim_training_course.sql', os.getenv('STAGING')], env=env)
        #result = runner.invoke(app, ['table', 'init-unit-tests', 'aqem_fct_step_role_assignee_relation'], env=env)
        #result = runner.invoke(app, ['table', 'build-inventory'], env=env)
        #result = runner.invoke(app, ['pipeline', 'build-metadata', os.getenv('PIPELINES') + '/stage/stage_tenant_dimension/dim_event_action_item/sql-scripts/dml.aqem_dim_event_action_item.sql'], env=env)
        #result = runner.invoke(app, ['table', 'run-unit-tests', 'aqem_dim_event_element', '--test-case-name', 'test_aqem_dim_event_element_1'], env=env)
        #result = runner.invoke(app, ['pipeline', 'deploy', '--product-name', 'aqem', '--max-thread' , 10, '--pool-creation'], env=env)
        #result = runner.invoke(app, ['pipeline', 'undeploy', '--product-name', 'aqem', '--no-ack'], env=env)
        #result = runner.invoke(app,['pipeline', 'build-all-metadata'], env=env)
        #result = runner.invoke(app, ['pipeline', 'prepare', os.getenv('PIPELINES') + '/alter_table_avro_dev.sql'], env=env)
        #result = runner.invoke(app, ['table', 'init-unit-tests',  '--nb-test-cases', '1', 'aqem_dim_event_element'], env=env)
        #result = runner.invoke(app, ['pipeline', 'analyze-pool-usage', '--directory', os.getenv('PIPELINES') + '/sources'], env=env)
        result = runner.invoke(app, ['pipeline', 'report', 'customer_analytics_c360'], env=env)
        print(result.stdout)




if __name__ == '__main__':
    unittest.main()
