import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
import re
"""
This is the file to keep to be able to debug step by step the code with access to CC.
"""

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


# Debug: Print key environment variables to verify they're loaded
if __name__ == '__main__' or True:  # Always print when debugging
    print("\n=== Environment Variables at Module Load ===")
    key_vars = ['FLINK_PROJECT', 'SL_CONFIG_FILE', 'PIPELINES', 'SRC_FOLDER', 'STAGING']
    for var in key_vars:
        value = os.environ.get(var, 'NOT SET')
        print(f"{var}: {value}")
    print("=" * 50 + "\n")

#os.environ["SL_CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
#data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
#os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
#os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")


from typer.testing import CliRunner
from shift_left.cli import app

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
        key_vars = ['FLINK_PROJECT', 'SL_CONFIG_FILE', 'PIPELINES', 'SRC_FOLDER', 'STAGING']
        print("\n=== Environment Variables Check ===")
        for var in key_vars:
            value = env.get(var, 'NOT SET')
            print(f"{var}: {value}")
        print("=" * 40 + "\n")

        #result = runner.invoke(app, ['pipeline', 'deploy', '--table-name', 'aqem_fct_event_action_item_assignee_user', '--force-ancestors', '--cross-product-deployment'], env=env)
        # result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'sl_c360_fct_user_per_group', '--compute-pool-id', os.getenv('SL_FLINK_COMPUTE_POOL_ID')], env=env)
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
        #result = runner.invoke(app, ['pipeline', 'report', 'customer_analytics_c360'], env=env)
        # result = runner.invoke(app, ['pipeline', 'report-running-statements', '--table-name', 'sl_c360_fct_user_per_group'], env=env)
        #result = runner.invoke(app, ['pipeline', 'prepare', os.getenv('PIPELINES') + '/test_prepare_tables_integration.sql', '--compute-pool-id', os.getenv('SL_FLINK_COMPUTE_POOL_ID')], env=env)
        result = runner.invoke(app, ['project', 'list-modified-files', 'develop', '--project-path', os.getenv('PIPELINES') + "/../../../../../../", '--file-filter', '.sql', '--since', '2026-05-01'], env=env)
        print(result.stdout)




if __name__ == '__main__':
    unittest.main()
