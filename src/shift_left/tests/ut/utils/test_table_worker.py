"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
from importlib import import_module
import os
from typing import Tuple
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
from shift_left.core.utils.app_config import get_config
import shift_left.core.table_mgr as tm
from shift_left.core.utils.table_worker import (
    TableWorker,
    ReplaceEnvInSqlContent,
    Change_CompressionType,
    Change_SchemaContext)

from shift_left.core.utils.file_search import list_src_sql_files

class TestTableWorker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")


    def test_update_dml_statement(self):
        print("Test update dml sql content")
        with open("test_file", "w") as f:
            f.write("insert into t3 select id,b,c from t2;")

        class TestUpdate(TableWorker):
            def update_sql_content(self, sql_in : str, string_to_change_from: str= None, string_to_change_to: str= None) -> Tuple[bool, str]:
                return True, sql_in.replace("from t2", "from t2 join t3 on t3.id = t2.id")

        updated = tm.update_sql_content_for_file(sql_file_name="test_file",
                                                 processor=TestUpdate(),
                                                 string_to_change_from="t2",
                                                 string_to_change_to="t2 join t3 on t3.id = t2.id")
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


    def test_Change_SchemaContext(self):
        sql_in="""
        create table T_schema_context (
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
        worker = Change_SchemaContext()
        updated, sql_out= worker.update_sql_content(sql_in)
        assert sql_out
        assert updated
        assert "'key.avro-registry.schema-context' = '.flink-dev'" in sql_out
        assert "'value.avro-registry.schema-context' = '.flink-dev'" in sql_out
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
        updated, sql_out= worker.update_sql_content(sql_in,'','p4')
        assert sql_out
        assert updated
        assert "'kafka.producer.compression.type' = 'snappy'" in sql_out
        print(sql_out)

    def test_schema_context_update_for_stage_env(self):
        module_path, class_name = "shift_left.core.utils.table_worker.ReplaceEnvInSqlContent".rsplit('.',1)
        get_config()['kafka']['cluster_type']='stage'
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        sql_in="""
        CREATE TABLE table_1 (
            a string
        ) WITH (
            'key.avro-registry.schema-context' = '.flink-dev',
            'value.avro-registry.schema-context' = '.flink-dev',
            'changelog.mode' = 'upsert',
            'kafka.retention.time' = '0',
            'scan.bounded.mode' = 'unbounded',
            'scan.startup.mode' = 'earliest-offset',
            'value.fields-include' = 'all',
            'key.format' = 'avro-registry',
            'value.format' = 'avro-registry'
        )
        """
        updated, sql_out= runner_class().update_sql_content(sql_in,'','p4')
        print(sql_out)
        assert updated
        assert "'key.avro-registry.schema-context' = '.flink-stage'" in sql_out
        assert "'value.avro-registry.schema-context' = '.flink-stage'" in sql_out

    def test_schema_context_update_for_prod_env(self):
        module_path, class_name = "shift_left.core.utils.table_worker.ReplaceEnvInSqlContent".rsplit('.',1)
        get_config()['kafka']['cluster_type']='prod'
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        sql_in="""
        CREATE TABLE table_1 (
            a string
        ) WITH (
            'key.avro-registry.schema-context' = '.flink-dev',
            'value.avro-registry.schema-context' = '.flink-dev',
            'changelog.mode' = 'upsert',
            'kafka.retention.time' = '0',
            'scan.bounded.mode' = 'unbounded',
            'scan.startup.mode' = 'earliest-offset',
            'value.fields-include' = 'all',
            'key.format' = 'avro-registry',
            'value.format' = 'avro-registry'
        )
        """
        updated, sql_out= runner_class().update_sql_content(sql_in,'','p4')
        print(sql_out)
        assert updated
        assert "'key.avro-registry.schema-context' = '.flink-prod'" in sql_out
        assert "'value.avro-registry.schema-context' = '.flink-prod'" in sql_out

    def test_dml_sql_content_update_for_stage_env(self):
        module_path, class_name = "shift_left.core.utils.table_worker.ReplaceEnvInSqlContent".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        get_config()['kafka']['cluster_type']='stage'
        get_config()['kafka']['src_topic_prefix']='replicated'
        sql_in="""
        INSERT INTO src_order
        SELECT
            id,
            status,
            name,
            after.is_migrated
        FROM
            `ap-tag-order-dev.template`
        );
        """
        updated, sql_out= runner_class().update_sql_content(sql_in,'','p4')
        print(sql_out)
        assert "replicated.stage.ap-tag-order-stage.template" in sql_out

    def test_dml_sql_content_update_for_prod_env(self):
        module_path, class_name = "shift_left.core.utils.table_worker.ReplaceEnvInSqlContent".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        get_config()['kafka']['cluster_type']='prod'
        get_config()['kafka']['src_topic_prefix']='replicated'
        sql_in="""
        INSERT INTO src_order
        SELECT
            id,
            status,
            name,
            after.is_migrated
        FROM
            `ap-tag-order-dev.template`
        );
        """
        updated, sql_out= runner_class().update_sql_content(sql_in,'','p4')
        print(sql_out)
        assert updated
        assert "replicated.prod.ap-tag-order-prod.template" in sql_out

    def test_regex_replace(self):
        import re

        sql_in="""
            ap-table-name-dev
            """
        sql_out = re.sub( r"^(.*?)(ap-.*?)-(dev)",r"\1clone.stage.\2", sql_in,flags=re.MULTILINE)
        assert "clone.stage.ap-table-name" in sql_out
        print(sql_out)
        sql_in_with_dot="""
            ap-table-name-dev.suffix
            """
        sql_out_with_dot = re.sub( r"^(.*?)(ap-.*?)-(dev)\.",r"\1clone.stage.\2-stage.", sql_in_with_dot,flags=re.MULTILINE)
        assert "clone.stage.ap-table-name-stage.suffix" in sql_out_with_dot # This assertion will now pass
        print(sql_out_with_dot)

        sql_out_with_dot_fixed = re.sub( r"^(.*?)(ap-.*?)\.",r"\1clone.stage.\2", sql_in_with_dot,flags=re.MULTILINE)
        assert "clone.stage.ap-table-name.suffix" not in sql_out_with_dot_fixed
        assert "clone.stage.ap-table-name" in sql_out_with_dot_fixed
        print(sql_out_with_dot_fixed)

    def test_change_concat_to_concat_ws(self):
        """Test changing MD5(CONCAT) to MD5(CONCAT_WS)"""
        sql_in = """
        SELECT MD5(CONCAT(id, name, status)) as hash_key
        FROM table1;
        """
        module_path, class_name = "shift_left.core.utils.table_worker.Change_Concat_to_Concat_WS".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "MD5(CONCAT_WS('''" in sql_out


    def test_default_string_replacement_in_from_clause(self):
        """Test string replacement in FROM clause"""
        sql_in = """
        SELECT * FROM table1;
        """
        module_path, class_name = "shift_left.core.utils.table_worker.DefaultStringReplacementInFromClause".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out = runner_class().update_sql_content(
            sql_content=sql_in,
            string_to_change_from="table1",
            string_to_change_to="table2"
        )
        assert updated
        assert "FROM table2" in sql_out
        print(sql_out)

    def test_change_mode_to_upsert_edge_cases(self):
        """Test edge cases for changing to upsert mode"""
        # Test with existing changelog.mode but different value
        sql_in = """
        CREATE TABLE test_table (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'changelog.mode' = 'append',
            'key.format' = 'avro-registry'
        )
        """
        module_path, class_name = "shift_left.core.utils.table_worker.ChangeChangeModeToUpsert".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "'changelog.mode' = 'upsert'" in sql_out


        # Test with no WITH clause
        sql_in = """
        CREATE TABLE test_table (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        )
        """
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "WITH (\n   'changelog.mode' = 'upsert'" in sql_out


    def test_change_pk_fk_to_sid_edge_cases(self):
        """Test edge cases for changing PK_FK to SID"""
        # Test with multiple occurrences
        sql_in = """
        CREATE TABLE test_table (
            id_pk_fk STRING,
            ref_pk_fk STRING,
            PRIMARY KEY (id_pk_fk) NOT ENFORCED
        )
        """
        module_path, class_name = "shift_left.core.utils.table_worker.ChangePK_FK_to_SID".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "id_sid" in sql_out
        assert "ref_sid" in sql_out

        # Test with no PK_FK
        sql_in = """
        CREATE TABLE test_table_no` (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        )
        """
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert not updated
        assert sql_out == sql_in

    def test_change_compression_type_edge_cases(self):
        """Test edge cases for changing compression type"""
        # Test with existing compression type but different value
        sql_in = """
        CREATE TABLE test_table_1 (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'kafka.producer.compression.type' = 'gzip',
            'key.format' = 'avro-registry'
        )
        """
        module_path, class_name = "shift_left.core.utils.table_worker.Change_CompressionType".rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "'kafka.producer.compression.type' = 'snappy'" in sql_out


        # Test with no WITH clause and closing parenthesis
        sql_in = """
        CREATE TABLE test_table_2 (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        );
        """
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "WITH (\n        'kafka.producer.compression.type' = 'snappy'" in sql_out
        assert sql_out.strip().endswith(");")


        # Test with no WITH clause and no semicolon
        sql_in = """
        CREATE TABLE test_table_3 (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        )
        """
        updated, sql_out = runner_class().update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "WITH (\n        'kafka.producer.compression.type' = 'snappy'" in sql_out
        assert sql_out.strip().endswith(");")


    def test_change_schema_context_edge_cases(self):
        """Test edge cases for changing schema context"""
        # Test with existing schema context but different value
        sql_in = """
        CREATE TABLE test_table (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'key.avro-registry.schema-context' = '.flink-prod',
            'value.avro-registry.schema-context' = '.flink-prod',
            'key.format' = 'avro-registry'
        )
        """
        worker = Change_SchemaContext()
        updated, sql_out = worker.update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert "'key.avro-registry.schema-context'='.flink-dev'" in sql_out
        assert "'value.avro-registry.schema-context'='.flink-dev'" in sql_out

        # Test with no WITH clause
        sql_in = """
        CREATE TABLE test_table (
            id STRING,
            PRIMARY KEY (id) NOT ENFORCED
        )
        """
        updated, sql_out = worker.update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert updated
        assert ") WITH" in sql_out
        assert "'key.avro-registry.schema-context' = '.flink-dev'" in sql_out


    def test_replace_env_in_sql_content_errors(self):
        """Test error cases for environment replacement"""
        # Test with invalid SQL content
        sql_in = "INVALID SQL CONTENT"
        worker = ReplaceEnvInSqlContent()
        updated, sql_out = worker.update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert not updated
        assert sql_out == sql_in

        # Test with empty SQL content
        sql_in = ""
        updated, sql_out = worker.update_sql_content(sql_content=sql_in)
        print(sql_out)
        assert not updated
        assert sql_out == sql_in

        # Test with None SQL content
        sql_in = None
        with self.assertRaises(Exception):
            worker.update_sql_content(sql_content=sql_in)

    def test_replace_topic_name_in_sql_content(self):
        """Test clone.dev is replace by clone.stage in sql content"""
        # Test with invalid SQL content
        sql_in = "insert into src_order select id, status, name, is_migrated from `clone.dev.ap-tag-order-dev.template`;"
        worker = ReplaceEnvInSqlContent()
        updated, sql_out = worker.update_sql_content(sql_content=sql_in, column_to_search='tenant_id', product_name='p4')
        print(sql_out)
        assert updated
        assert "replicated.stage.ap-tag-order-stage.template" in sql_out

    def test_replace_topic_name_in_sql_content_for_prod_env(self):
        """Test clone.dev is replace by clone.prod in sql content"""
        # Test with invalid SQL content
        sql_in = "insert into src_order select id, status, name, is_migrated from `clone.dev.ap-tag-order-dev.template`;"

        get_config()['kafka']['cluster_type']='prod'
        get_config()['kafka']['src_topic_prefix']='replicated'
        worker = ReplaceEnvInSqlContent()
        updated, sql_out = worker.update_sql_content(sql_content=sql_in, column_to_search='tenant_id', product_name='p4')
        print(sql_out)
        assert updated
        assert "replicated.prod.ap-tag-order-prod.template" in sql_out

    def test_change_data_limit_where_condition(self):
        """Test change data limit where condition"""
        sql_content = """
        INSERT INTO src_l_metadata
        with final as (
            SELECT
                COALESCE(IF(op = 'd', before.user_detail_id, after.user_detail_id), 'NULL') as `user_detail_id`,
                COALESCE(IF(op = 'd', before.metadata_id, after.metadata_id), 'NULL') as metadata_id,
                op,
                tenant_id,
                source.lsn as source_lsn
            FROM `ap-.user_detail_metadata`
        )
        SELECT *  FROM final
        """
        print("\n1-should change the content as the config is a dev environment, there is the expected column to filter on and it is a source table")
        transformer = ReplaceEnvInSqlContent()
        updated, sql_out= transformer.update_sql_content(sql_content, "tenant_id", "p4")
        assert " WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = 'p4'" in sql_out
        assert updated
        print(sql_out)

        print("\n2-should change the content as it is a dev environment, there is the expected column to filter on, it is a source table, just other case to assess pattern matching works.")
        sql_content_2=sql_content.replace("INSERT INTO", "insert into")
        updated, sql_out= transformer.update_sql_content(sql_content_2, "tenant_id", "p4")
        assert "WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = 'p4'" in sql_out
        assert updated

        print("\n3-should not change the content as it is not a source table")
        sql_content_3=sql_content.replace("src_", "int_")
        updated, sql_out= transformer.update_sql_content(sql_content_3, "tenant_id", "p4")
        print(sql_out)
        assert not " WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = 'p4'" in sql_out
        assert not updated

        print("\n4-should not change the content as there is not the expected column to filter on")
        sql_content_4=sql_content.replace("tenant_id", "user_id")
        updated, sql_out= transformer.update_sql_content(sql_content_4, "tenant_id", "p4")
        print(sql_out)
        assert not "WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = 'p4'" in sql_out
        assert not updated

        print("\n5-should not change the content as it is a stage environment")
        # NOT A dev environment no changes.
        config = get_config()
        config['kafka']['cluster_type'] = "stage"
        transformer = ReplaceEnvInSqlContent()
        updated, sql_out= transformer.update_sql_content(sql_content, "tenant_id", "p4")
        assert not updated
        assert not "WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = 'p4'" in sql_out


    def test_validate_only_src_table_is_updated_with_data_limit_logic(self):
        """Test change data limit where condition is only applied to src_ tables"""
        sql_content = """
        INSERT INTO dim_l_metadata
        with final as (
            SELECT
                a,b,c,d, tenant_id
            FROM `user_detail_metadata`
        )
        SELECT *  FROM final
        """
        print("test_validate_only_src_table_is_updated_with_data_limit_logic")
        print("\nshould not change the content even as this is a dim table")
        transformer = ReplaceEnvInSqlContent()
        updated, sql_out= transformer.update_sql_content(sql_content, "tenant_id", "p4")
        assert " WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = 'p4'" not in sql_out
        assert not updated
        print(sql_out)




if __name__ == '__main__':
    unittest.main()
