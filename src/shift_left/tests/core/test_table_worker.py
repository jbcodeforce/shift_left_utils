import unittest
import pathlib
from importlib import import_module 
import os
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
      
import shift_left.core.table_mgr as tm
from shift_left.core.utils.table_worker import TableWorker
from shift_left.core.utils.file_search import list_src_sql_files

class TestTableWorker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")

    def test_update_ddl_statement(self):
        table_name = "int_table_1"
        print("Test update ddl sql content")
        files = list_src_sql_files(os.getenv("PIPELINES")+ "/facts/p1/fct_order")
        sql_in = tm.load_sql_content(files["ddl.fct_order"])
        class TestUpdate(TableWorker):
            def update_sql_content(sql_in : str):
                return sql_in.replace(" 'kafka.retention.time' = '0',", " 'kafka.retention.time' = '0', \n'sql.local-time-zone' = 'UTC-0',")
        sql_out=tm.update_sql_content_for_file(sql_in, TestUpdate)
        assert "sql.local-time-zone" in sql_out
        print(sql_out)
    
    def test_insert_upsert(self):
        sql_in="""
        create table T (
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
        sql_out = tm.update_sql_content_for_file(sql_in,runner_class)
        assert sql_out
        assert "'changelog.mode' = 'upsert'" in sql_out
        print(sql_out)
    
    def test_upsert_update(self):
        sql_in="""
        create table T (
           id string,
           a string,
           primary key (id) not enforced
        ) with (
           'changelog.mode' = 'append'
        )
        """
        module_path, class_name = "shift_left.core.utils.table_worker.ChangeChangeModeToUpsert".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        sql_out = tm.update_sql_content_for_file(sql_in,runner_class)
        assert sql_out
        assert "'changelog.mode' = 'upsert'" in sql_out
        print(sql_out)

    def test_pf_dk_update(self):
        sql_in="""
        create table T (
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
        sql_out = tm.update_sql_content_for_file(sql_in,runner_class)
        assert sql_out
        assert "'a_sid" in sql_out
        print(sql_out)


if __name__ == '__main__':
    unittest.main()