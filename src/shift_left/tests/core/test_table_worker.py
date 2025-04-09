import unittest
import pathlib
from importlib import import_module 
import os
from typing import Tuple
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
      
import shift_left.core.table_mgr as tm
from shift_left.core.utils.table_worker import (
    TableWorker, 
    Change_CompressionType, 
    Change_Concat_to_Concat_WS,
    ChangeLocalTimeZone,
    ChangeChangeModeToUpsert,
    ChangePK_FK_to_SID)

from shift_left.core.utils.file_search import list_src_sql_files

class TestTableWorker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")

    def test_update_retention_ddl_statement(self):
        print("Test update ddl sql content")
        files = list_src_sql_files(os.getenv("PIPELINES")+ "/facts/p1/fct_order")
        class TestUpdate(TableWorker):
            def update_sql_content(self, sql_in : str) -> Tuple[bool, str]:
                return True, sql_in.replace(" 'kafka.retention.time' = '0',", " 'kafka.retention.time' = '0', \n'sql.local-time-zone' = 'UTC-0',")
        worker= TestUpdate()
        updated=tm.update_sql_content_for_file(files["ddl.fct_order"], worker)
        assert updated
        with open(files["ddl.fct_order"], "r") as f:
            sql_out = f.read()
            assert "'kafka.retention.time' = '0" in sql_out
            print(sql_out)
    
    def test_update_dml_statement(self):
        print("Test update dml sql content")
        with open("test_file", "w") as f:
            f.write("insert into t3 select id,b,c from t2;")

        class TestUpdate(TableWorker):
            def update_sql_content(self, sql_in : str) -> Tuple[bool, str]:
                return True, sql_in.replace("from t2", "from t2 join t3 on t3.id = t2.id")
        
        updated = tm.update_sql_content_for_file("test_file", TestUpdate())
        assert updated
        with open("test_file", "r") as f:
            assert f.read() == "insert into t3 select id,b,c from t2 join t3 on t3.id = t2.id;"

        os.remove("test_file")

    def test_insert_upsert(self):
        sql_in="""
        create table Tinsert_upsert (
           id string,
           a string,
           primary key (id) not enforced
        ) WITH (
            'key.format' = 'avro-registry',
            'value.format' = 'avro-registry'
        );
        """
        module_path, class_name = "shift_left.core.utils.table_worker.ChangeChangeModeToUpsert".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out= runner_class().update_sql_content(sql_in)
        assert sql_out
        assert updated
        assert "'changelog.mode' = 'upsert'" in sql_out
        print(sql_out)
    
    def test_upsert_update(self):
        sql_in="""
        create table Tupdatechangemode (
           id string,
           a string,
           primary key (id) not enforced
        ) with (
           'changelog.mode' = 'append',
            'key.format' = 'avro-registry',
            'value.format' = 'avro-registry',
            'value.fields-include' = 'all'

        )
        """
        module_path, class_name = "shift_left.core.utils.table_worker.ChangeChangeModeToUpsert".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out= runner_class().update_sql_content(sql_in)
        assert sql_out
        assert updated
        assert "'changelog.mode' = 'upsert'" in sql_out
        print(sql_out)

    def test_pf_dk_update(self):
        sql_in="""
        create table Tpk_fk (
           id string,
           a_pk_fk string,
           primary key (id) not enforced
        ) with (
           'changelog.mode' = 'append'
        )
        """
        module_path, class_name = "shift_left.core.utils.table_worker.ChangePK_FK_to_SID".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out= runner_class().update_sql_content(sql_in)
        assert sql_out
        assert "a_sid" in sql_out
        print(sql_out)

    def test_ChangeLocalTimeZone(self):
        sql_in="""
        create table T_timezone (
           id string,
           a_pk_fk string,
           primary key (id) not enforced
        ) with (
           'changelog.mode' = 'append',
            'key.format' = 'avro-registry',
            'value.format' = 'avro-registry',
            'value.fields-include' = 'all'
        )
        """
        worker = ChangeLocalTimeZone()
        updated, sql_out= worker.update_sql_content(sql_in)
        assert sql_out
        assert updated
        assert "'sql.local-time-zone'" in sql_out
        print(sql_out)

    def test_Change_CompressionType(self):
        sql_in="""
        create table Tcompress_type (
           id string,
           a_pk_fk string,
           primary key (id) not enforced
        ) with (
           'changelog.mode' = 'append'
        )
        """
        worker = Change_CompressionType()
        updated, sql_out= worker.update_sql_content(sql_in)
        assert sql_out
        assert updated
        assert "'kafka.producer.compression.type'='snappy'" in sql_out
        print(sql_out)

if __name__ == '__main__':
    unittest.main()