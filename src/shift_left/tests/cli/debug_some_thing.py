from shift_left.cli_commands.pipeline import report, build_metadata
import os

if __name__ == "__main__":
    pipe_path = os.getenv("PIPELINES")
    #report("stage_tenant_dimension", pipe_path, to_graph = True)
    build_metadata(pipe_path + "/dimensions/mx/dim_role_group_location/sql-scripts/dml.mx_dim_role_group_location.sql", pipe_path)