"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil

import pathlib
from unittest.mock import patch, mock_open, MagicMock, call
from datetime import datetime, timezone, timedelta
import subprocess
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
from shift_left.core.utils.app_config import get_config
import shift_left.core.project_manager as pm
from shift_left.core.project_manager import ModifiedFileInfo
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm
from shift_left.core.models.flink_statement_model import Statement, Spec, Status
from ut.core.BaseUT import BaseUT

class TestProjectManager(BaseUT):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = str(Path(__file__).parent / "../tmp")  # Path to the tmp directory
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES"))
        pipemgr.build_all_pipeline_definitions( os.getenv("PIPELINES"))


    def _backup_file(self, sql_name: str):
        sql_path = os.path.join(os.getenv("PIPELINES"), sql_name.lstrip("/"))
        backup_path = sql_path + ".bak"
        shutil.copy2(sql_path, backup_path)
        return backup_path

    def _restore_file(self, backup_path: str):
        sql_path =backup_path.replace(".bak", "")
        shutil.copy2(backup_path, sql_path)
        os.remove(backup_path)

    def test_update_version_view_table(self):
        sql_name="/facts/p2/f/sql-scripts/dml.f.sql"
        # Backup the file before test so we can restore it afterwards
        sql_path = os.path.join(os.getenv("PIPELINES"), sql_name.lstrip("/"))
        backup_path = self._backup_file(sql_name)
        f_info = ModifiedFileInfo(table_name="f", file_modified_url=sql_name, same_sql_content=False, running=False, new_table_name="f")
        file_info_list = [f_info]
        pm.update_tables_version(file_info_list, "_v2")
        with open(sql_path, 'r') as f:
            sql_content = f.read()
        assert "f_v2" in sql_content
        # After the version update, restore the original file from backup to avoid side effects for other tests
        self._restore_file(backup_path)


    def test_build_new_table_name(self):
        new_table_name = pm._build_new_table_name("f_v2", "_v2")
        assert new_table_name == "f_v3"
        new_table_name = pm._build_new_table_name("f", "_v2")
        assert new_table_name == "f_v2"
        new_table_name = pm._build_new_table_name("f_v4", "_v2")
        assert new_table_name == "f_v5"
        new_table_name = pm._build_new_table_name("a_b_f_v4", "_v2")
        assert new_table_name == "a_b_f_v5"


    def test_update_version_intermediate_table(self):
        """
        Modifying Z, means changing D,P,C then E,F
        """
        # Prepare
        z_sql_name="/intermediates/p2/z/sql-scripts/dml.z.sql"
        z_dml_backup=self._backup_file(z_sql_name)
        z_ddl_backup=self._backup_file("/intermediates/p2/z/sql-scripts/ddl.z.sql")
        d_dml_backup=self._backup_file("/intermediates/p2/d/sql-scripts/dml.d.sql")
        d_ddl_backup=self._backup_file("/intermediates/p2/d/sql-scripts/ddl.d.sql")
        c_dml_backup=self._backup_file("/intermediates/p2/c/sql-scripts/dml.c.sql")
        c_ddl_backup=self._backup_file("/intermediates/p2/c/sql-scripts/ddl.c.sql")
        p_dml_backup=self._backup_file("/facts/p2/p/sql-scripts/dml.p.sql")
        p_ddl_backup=self._backup_file("/facts/p2/p/sql-scripts/ddl.p.sql")
        e_dml_backup=self._backup_file("/facts/p2/e/sql-scripts/dml.e.sql")
        e_ddl_backup=self._backup_file("/facts/p2/e/sql-scripts/ddl.e.sql")
        f_dml_backup=self._backup_file("/facts/p2/f/sql-scripts/dml.f.sql")
        f_ddl_backup=self._backup_file("/facts/p2/f/sql-scripts/ddl.f.sql")
        # Call
        z_info = ModifiedFileInfo(table_name="z", file_modified_url=z_sql_name, same_sql_content=False, running=False, new_table_name="")
        file_info_list = [z_info]
        pm.update_tables_version(file_info_list, "_v2")
        # Validate
        with open(os.path.join(os.getenv("PIPELINES"),z_sql_name.lstrip("/")), 'r') as f:
            sql_content = f.read()
        assert "z_v2" in sql_content
        # verify children tables are updated
        p_sql_path = os.path.join(os.getenv("PIPELINES"), "/facts/p2/p/sql-scripts/dml.p.sql".lstrip("/"))
        with open(p_sql_path, 'r') as f:
            sql_content = f.read()
            assert "z_v2" in sql_content
            assert "p_v2" in sql_content
        c_sql_path = os.path.join(os.getenv("PIPELINES"), "/intermediates/p2/c/sql-scripts/dml.c.sql".lstrip("/"))
        with open(c_sql_path, 'r') as f:
            sql_content = f.read()
            assert "z_v2" in sql_content
            assert "c_v2" in sql_content
        e_sql_path = os.path.join(os.getenv("PIPELINES"), "/facts/p2/e/sql-scripts/dml.e.sql".lstrip("/"))
        with open(e_sql_path, 'r') as f:
            sql_content = f.read()
            assert "e_v2" in sql_content
            assert "c_v2" in sql_content
        self._restore_file(z_dml_backup)
        self._restore_file(z_ddl_backup)
        self._restore_file(d_dml_backup)
        self._restore_file(d_ddl_backup)
        self._restore_file(c_dml_backup)
        self._restore_file(c_ddl_backup)
        self._restore_file(p_dml_backup)
        self._restore_file(p_ddl_backup)
        self._restore_file(e_dml_backup)
        self._restore_file(e_ddl_backup)
        self._restore_file(f_dml_backup)
        self._restore_file(f_ddl_backup)


if __name__ == '__main__':
    unittest.main()
