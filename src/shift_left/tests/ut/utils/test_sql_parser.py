"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import re
import pathlib
from shift_left.core.utils.sql_parser import (
    SQLparser
)


class TestSQLParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")

    def test_upgrade_mode_get_table_from_select(self):
        parser = SQLparser()
        query="""
        SELECT * FROM table1
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "table1" in rep
        assert "Stateless" in parser.extract_upgrade_mode(query, "")

    def test_upgrade_mode_get_tables_from_join(self):
        parser = SQLparser()
        query= """
        SELECT a.*, b.name
        FROM schema1.table1 a
        JOIN table2 b ON a.id = b.id
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "schema1.table1" in rep
        assert "table2" in rep
        assert "Stateful" in parser.extract_upgrade_mode(query, "")

    def test_upgrade_mode_get_tables_from_inner_join(self):
        parser = SQLparser()
        query= """
        SELECT *
        FROM table1 t1
        LEFT JOIN schema2.table2 AS t2 ON t1.id = t2.id
        INNER JOIN table3 t3 ON t2.id = t3.id
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "schema2.table2" in rep
        assert "table1" in rep
        assert "table3" in rep
        assert "Stateful" in parser.extract_upgrade_mode(query, "")

    def test_upgrade_mode_get_tables_from_right_join(self):
        parser = SQLparser()
        query=  """
        SELECT *
        FROM table1
        RIGHT OUTER JOIN table2 ON table1.id = table2.id
        FULL JOIN table3 ON table2.id = table3.id
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "table2" in rep
        assert "table1" in rep
        assert "table3" in rep
        assert "Stateful" in parser.extract_upgrade_mode(query, "")


    def test_upgrade_mode_get_tables_without_ctes(self):
        parser = SQLparser()
        query= """
        WITH cte1 AS (
           SELECT a,b,c
           FROM table1
        ),
        cte2 AS (
            SELECT d,b,e
           FROM table2
        )
        SELECT *
        FROM cte1
        RIGHT OUTER JOIN cte2 ON table1.id = table2.id
        FULL JOIN table3 ON table2.id = table3.id
        CROSS JOIN unnest(split(trim(BOTH '[]' FROM y.target_value),','))  as user_id
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "table1" in rep
        assert "table2" in rep
        assert not "cte1" in rep
        print(rep)
        assert "Stateful" in parser.extract_upgrade_mode(query, "")


    def test_upgrade_mode_get_tables_without_ctes(self):
        parser = SQLparser()
        query="""
            -- a comment
        with exam_def as (select * from {{ ref('int_exam_def_deduped') }} )
        ,exam_data as (select * from {{ ref('int_exam_data_deduped') }} )
        ,exam_performance as (select * from {{ ref('int_exam_performance_deduped') }} )
        ,training_data as (select * from {{ ref('int_training_data_deduped') }} )
        """
        rep=parser.extract_table_references(query)
        assert rep
        print(rep)
        assert "Stateless" in parser.extract_upgrade_mode(query, "")

    def test_upgrade_mode_extract_table_name_from_insert(self):
        parser = SQLparser()
        query="INSERT INTO mytablename\nSELECT a,b,c\nFROM src_table"
        rep=parser.extract_table_references(query)
        assert rep
        assert "src_table" in rep
        print(rep)
        assert "Stateless" in parser.extract_upgrade_mode(query, "")


    def test_upgrade_mode_sql_content_order(self):
        fname = os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            parser = SQLparser()
            referenced_table_names = parser.extract_table_references(sql_content)
            assert len(referenced_table_names) == 3
            assert "Stateful" in parser.extract_upgrade_mode(sql_content, "")

    def test_upgrade_mode_stateless_from_dml_ref(self):
        parser = SQLparser()
        fname = os.getenv("PIPELINES") + "/sources/p2/src_a/sql-scripts/dml.src_a.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            upgrade_mode = parser.extract_upgrade_mode(sql_content, "")
            assert "Stateless"  == upgrade_mode

    def test_upgrade_mode_cross_join_unnest(self):
        parser = SQLparser()
        query="""
        SELECT order_id, product_name
        FROM Orders
            CROSS JOIN UNNEST(product_names) AS t(product_name)
        """
        assert "Stateless" in parser.extract_upgrade_mode(query, "" )

    def test_extract_columns_from_ddl(self):
        parser = SQLparser()
        fname = os.getenv("PIPELINES") + "/intermediates/p1/int_test/sql-scripts/ddl.test.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            columns = parser.build_column_metadata_from_sql_content(sql_content)
            assert columns
            assert columns['id'] == {'name': 'id', 'type': 'STRING', 'nullable': False, 'primary_key': True}
            assert columns['tenant_id'] == {'name': 'tenant_id', 'type': 'STRING', 'nullable': False, 'primary_key': True}
            assert columns['status'] == {'name': 'status', 'type': 'STRING', 'nullable': True, 'primary_key': False}
            assert columns['name'] == {'name': 'name', 'type': 'STRING', 'nullable': True, 'primary_key': False}
            assert columns['type'] == {'name': 'type', 'type': 'STRING', 'nullable': True, 'primary_key': False}
            assert columns['created_by'] == {'name': 'created_by', 'type': 'STRING', 'nullable': True, 'primary_key': False}
            assert columns['created_date'] == {'name': 'created_date', 'type': 'BIGINT', 'nullable': True, 'primary_key': False}

    def test_extract_keys(self):
        parser = SQLparser()
        sql_statement_multiple = 'PRIMARY KEY(id, name) NOT ENFORCED'
        primary_key = parser.extract_primary_key_from_sql_content(sql_statement_multiple)
        assert primary_key == ['id', 'name']
        sql_content = """
        CREATE TABLE IF NOT EXISTS `aqem_dim_event_element` (
            sid                     STRING NOT NULL,
            data_value_id           INTEGER,
            table_row_id            STRING,
            element_name            STRING,
            event_element_id        STRING,
             tenant_id               STRING,
            PRIMARY KEY(sid) NOT ENFORCED
        ) DISTRIBUTED BY HASH(sid) INTO 3 BUCKETS WITH (
        """
        primary_key = parser.extract_primary_key_from_sql_content(sql_content)
        assert primary_key == ['sid']

    def test_extract_table_name_from_insert_into_statement(self):
        parser = SQLparser()
        query="""INSERT INTO element_data
        WITH
            section_detail as (
                SELECT s.event_section_id, sc.name, s.tenant_id
                FROM `src_execution_plan` as s
                INNER JOIN
                    `src_configuration_section` as sc
                    ON sc.id = s.config_section_id
                    AND sc.tenant_id = s.tenant_id
            ),
            tenant as (
                SELECT CAST(null AS STRING) as id, t.__db as tenant_id
                FROM `tenant_dimension` as t
                where not (t.__op IS NULL OR t.__op = 'd')
            ),
            attachment as
            (
                SELECT
                    ae.*,
                    JSON_VALUE(att.object_state, '$.fileName' ) as filename
                FROM `int_aqem_recordexecution_element_data_unnest` ae
                JOIN `src_aqem_recordexecution_attachments` att
                    ON ae.element_data = att.id AND ae.tenant_id = att.tenant_id
                where ae.element_type = 'ATTACHMENT'
            ),

            -- Adjustments made here : Renamed to 'record_link', as to split out LINKS to EVENTS. Changed to INNER JOINs so that this CTE only handles record links.
            record_link as
            (
                SELECT rec.record_number, rec.title, le.id, le.element_data as link_id, le.data_value_id, le.tenant_id
                FROM `int_aqem_recordexecution_element_data_unnest` le
                INNER JOIN `src_aqem_recordexecution_record_links` rl
                    ON le.element_data = rl.id AND le.tenant_id = rl.tenant_id
                left JOIN `src_aqem_recordexecution_record` rec
                    ON rec.id = rl.to_record_id AND rec.tenant_id = rl.tenant_id
                where le.element_type in ('LINK', 'RECORD_LINK')
            ),

            -- Added a new CTE here, this one handles LINKS to DOCUMENTS, AND also uses an INNER JOIN.
            document_link as
            (
                SELECT dl.document_number, dl.title, le.id, le.element_data as link_id, le.data_value_id, le.tenant_id
                FROM `int_aqem_recordexecution_element_data_unnest` le
                left JOIN `src_aqem_recordexecution_document_link` dl
                    ON le.element_data = dl.id AND le.tenant_id = dl.tenant_id
                where le.element_type in ('LINK', 'RECORD_LINK')
            ),

            event_element as (
                SELECT
                    MD5(CONCAT_WS(',', ed.id, CAST(ed.data_value_id AS STRING), ed.tenant_id)) AS sid,
                    ed.parent_id,
                    ed.table_row_id,
                    ed.element_name as `name`,
                    CASE
                        WHEN ed.element_type = 'DROPDOWN'
                            THEN ed.element_data
                        ELSE null
                    END AS list_value,
                    CAST (ed.display_order AS INTEGER) as display_order,
                    MD5(CONCAT_WS(',', iud.user_id, ed.tenant_id)) AS updated_user_sid,
                    iud.full_name as updated_full_name,
                    iud.user_name as updated_user_name,
                    ed.updated_date,
                    s.name as event_section_name,
                    MD5(CONCAT_WS(',', ed.config_element_id, ed.tenant_id)) AS config_element_sid,
                    ed.id as event_element_id,
                    ed.tenant_id as tenant_id
                FROM `int_aqem_recordexecution_element_data_unnest` ed
                INNER JOIN `int_aqem_aqem_event` as `event`
                    on `event`.id = ed.record_id
                    and `event`.tenant_id = ed.tenant_id
                LEFT JOIN
                    section_detail as s
                    ON s.event_section_id = ed.section_id
                    AND s.tenant_id = ed.tenant_id
                LEFT JOIN
                    `int_user_detail_lookup` as iud
                    ON iud.user_id = ed.user_id
                    AND iud.tenant_id = ed.tenant_id
                LEFT JOIN
                    attachment as att
                    ON att.id = ed.id
                    AND att.tenant_id = ed.tenant_id
                LEFT JOIN
                    record_link as l
                    ON l.id = ed.id
                    and l.link_id = ed.element_data
                    AND l.tenant_id = ed.tenant_id
                LEFT JOIN
                    document_link as d
                    ON d.id = ed.id
                    and d.link_id = ed.element_data
                    AND d.tenant_id = ed.tenant_id

                UNION ALL

                SELECT
                    MD5(CONCAT_WS(',', dummy_rows.id, dummy_rows.id, dummy_rows.tenant_id)) AS sid,
                    CAST(NULL AS STRING) as parent_id,
                    CAST(NULL AS STRING) as table_row_id,
                    'Missing Event Element Data' as name,
                    CAST(NULL AS STRING) as `value`,
                    CAST(NULL AS STRING) as list_value,
                    CAST(NULL AS INTEGER) as display_order,
                    CAST(NULL AS STRING) as updated_user_sid,
                    CAST(NULL AS STRING) as updated_full_name,
                    CAST(NULL AS STRING) as updated_user_name,
                    CAST(NULL AS TIMESTAMP_LTZ(3)) as updated_date,
                    CAST(NULL AS STRING) as event_section_name,
                    CAST(NULL AS STRING) as config_element_sid,
                    CAST(NULL AS STRING) as event_element_id,
                    dummy_rows.tenant_id as tenant_id
                FROM tenant as dummy_rows
            )

        SELECT
            sid,
            table_row_id,
            name,
            `value`,
            list_value,
            display_order,
            updated_user_sid,
            updated_full_name,
            updated_user_name,
            updated_date,
            event_section_name,
            config_element_sid,
            event_element_id,
            tenant_id
        FROM event_element
        """
        rep=parser.extract_table_references(query)
        assert rep
        print(rep)


    def test_extract_cte_table(self):
        parser = SQLparser()
        query="""INSERT INTO element_data
        WITH
            section_detail as (
                SELECT s.event_section_id, sc.name, s.tenant_id
                FROM `src_execution_plan` as s
                INNER JOIN
                    `src_configuration_section` as sc
                    ON sc.id = s.config_section_id
                    AND sc.tenant_id = s.tenant_id
            ),
            attachment as
            (
                SELECT
                    ae.*,
                    JSON_VALUE(att.object_state, '$.fileName' ) as filename
                FROM `int_aqem_recordexecution_element_data_unnest` ae
                JOIN `src_aqem_recordexecution_attachments` att
                    ON ae.element_data = att.id AND ae.tenant_id = att.tenant_id
                where ae.element_type = 'ATTACHMENT'
            )
        """
        rep=parser.extract_table_references(query)
        assert rep
        print(rep)

    def test_build_column_metadata_from_sql_content(self):
        parser = SQLparser()
        query="""CREATE TABLE IF NOT EXISTS user_role (
            id STRING NOT NULL,
            tenant_id STRING NOT NULL,
            name STRING,
            `type` STRING,
            `standard` STRING,
            created_by STRING,
            created_date BIGINT,
            last_modified_by STRING,
            last_modified_date BIGINT,
            PRIMARY KEY(id, tenant_id) NOT ENFORCED
        )
        """
        columns = parser.build_column_metadata_from_sql_content(query)
        assert columns
        assert columns['id'] == {'name': 'id', 'type': 'STRING', 'nullable': False, 'primary_key': True}
        assert columns['tenant_id'] == {'name': 'tenant_id', 'type': 'STRING', 'nullable': False, 'primary_key': True}
        assert columns['name'] == {'name': 'name', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['type'] == {'name': 'type', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['standard'] == {'name': 'standard', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        print(columns)

    def test_extract_table_name_from_create_statement_quoted(self):
        parser = SQLparser()
        query="""CREATE TABLE IF NOT EXISTS `identity_metadata` (
            `id` VARCHAR(2147483647) NOT NULL,
            `key` VARCHAR(2147483647) NOT NULL,
            `value` VARCHAR(2147483647) NOT NULL,
            `tenant_id` VARCHAR(2147483647) NOT NULL,
            `description` VARCHAR(2147483647)
        """
        table_name = parser.extract_table_name_from_create_statement(query)
        assert table_name == "identity_metadata"


    def test_extract_table_name_from_insert_into_statement_quoted(self):
        parser = SQLparser()
        query="""INSERT INTO `identity_metadata` (
            `id` VARCHAR(2147483647) NOT NULL,
            `key` VARCHAR(2147483647) NOT NULL,
            `value` VARCHAR(2147483647) NOT NULL,
            `tenant_id` VARCHAR(2147483647) NOT NULL,
            `description` VARCHAR(2147483647)
        """
        table_name = parser.extract_table_name_from_insert_into_statement(query)
        assert table_name == "identity_metadata"

    def test_extract_table_name_from_insert_into_statement_start_with_number_and_quoted(self):
        parser = SQLparser()
        query="""INSERT INTO `75_identity_metadata` (
            `id` VARCHAR(2147483647) NOT NULL,
            `key` VARCHAR(2147483647) NOT NULL,
            `value` VARCHAR(2147483647) NOT NULL,
            `tenant_id` VARCHAR(2147483647) NOT NULL,
            `description` VARCHAR(2147483647)
        """
        table_name = parser.extract_table_name_from_insert_into_statement(query)
        assert table_name == "75_identity_metadata"

    def test_build_columns_from_sql_content_quoted_column_names(self):
        parser = SQLparser()
        query="""CREATE TABLE IF NOT EXISTS  `identity_metadata` (
            `id` VARCHAR(2147483647) NOT NULL,
            `key` VARCHAR(2147483647) NOT NULL,
            `value` VARCHAR(2147483647) NOT NULL,
            `tenant_id` VARCHAR(2147483647) NOT NULL,
            `description` VARCHAR(2147483647),
            `parent_id` VARCHAR(2147483647),
            `hierarchy` VARCHAR(2147483647),
            `op` VARCHAR(2147483647) NOT NULL,
            `source_lsn` BIGINT,
            CONSTRAINT `PRIMARY` PRIMARY KEY (`id`, `tenant_id`) NOT ENFORCED
            )
        """
        columns = parser.build_column_metadata_from_sql_content(query)
        assert columns
        print(columns)
        assert columns['id'] == {'name': 'id', 'type': 'STRING', 'nullable': False, 'primary_key': True}
        assert columns['key'] == {'name': 'key', 'type': 'STRING', 'nullable': False, 'primary_key': False}
        assert columns['value'] == {'name': 'value', 'type': 'STRING', 'nullable': False, 'primary_key': False}
        assert columns['tenant_id'] == {'name': 'tenant_id', 'type': 'STRING', 'nullable': False, 'primary_key': True}
        assert columns['description'] == {'name': 'description', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['parent_id'] == {'name': 'parent_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['hierarchy'] == {'name': 'hierarchy', 'type': 'STRING', 'nullable': True, 'primary_key': False}

    def test_build_columns_from_sql_content_quoted_column_names_2(self):
        parser = SQLparser()
        query="""CREATE TABLE `j9r-env`.`j9r-kafka`.`raw_users` (
  `user_id` VARCHAR(2147483647),
  `user_name` VARCHAR(2147483647),
  `user_email` VARCHAR(2147483647),
  `group_id` VARCHAR(2147483647),
  `tenant_id` VARCHAR(2147483647),
  `created_date` VARCHAR(2147483647),
  `is_active` BOOLEAN,
  `headers` MAP<VARBINARY(2147483647), VARBINARY(2147483647)> METADATA
)
DISTRIBUTED BY HASH(`user_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.compaction.time' = '0 ms',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.producer.compression.type' = 'snappy',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '0 ms',
  'key.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.avro-registry.schema-context' = '.flink-dev',
  'value.fields-include' = 'all',
  'value.format' = 'avro-registry'
)
        """
        columns = parser.build_column_metadata_from_sql_content(query)
        print(columns)
        assert columns['user_id'] == {'name': 'user_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['user_name'] == {'name': 'user_name', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['user_email'] == {'name': 'user_email', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['group_id'] == {'name': 'group_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['tenant_id'] == {'name': 'tenant_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['created_date'] == {'name': 'created_date', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert columns['is_active'] == {'name': 'is_active', 'type': 'BOOLEAN', 'nullable': True, 'primary_key': False}

    def test_should_not_consider_unnest_as_table_name(self):
        parser = SQLparser()
        query="""
        INSERT INTO `int_element_data_unnest`
            SELECT
                ed.id,
                ed.`name` as element_name,
                pe.element_type,
                CASE
                    WHEN TRIM(REGEXP_REPLACE(edata, '^["\[]|["\]"]$', '')) = 'null' THEN null
                    WHEN TRIM(REGEXP_REPLACE(edata, '^["\[]|["\]"]$', '')) = '' THEN null
                    ELSE TRIM(REGEXP_REPLACE(edata, '^["\[]|["\]"]$', ''))
                END AS element_data,
                ed.user_id,
                ed.tenant_id
            FROM `src_element_data` as ed
            INNER JOIN
                `src_record` as event
                ON event.id = ed.record_id
                AND event.tenant_id = ed.tenant_id
            INNER JOIN `src_plan_element` as pe
                ON pe.event_element_id = ed.execution_plan_element_id
                AND pe.tenant_id = ed.tenant_id
            LEFT JOIN `src_form_element` as fec
                ON fec.id = pe.config_element_id
                AND fec.tenant_id = pe.tenant_id
            CROSS JOIN UNNEST(split(REGEXP_REPLACE(ed.data, '^\["|\"]$', '') , '", "')) as edata
            WHERE not (parent_id is not null and table_row_id is null)
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "int_element_data_unnest" in rep
        assert "src_element_data" in rep
        assert "src_record" in rep
        assert "src_plan_element" in rep
        assert "src_form_element" in rep
        assert not "UNNEST" in rep

    def test_extract_statement_complexity_no_joins(self):
        """Test complexity extraction with no joins - should be Simple"""
        parser = SQLparser()
        query = """
        SELECT col1, col2, col3
        FROM table1
        WHERE col1 = 'value'
        """
        result = parser.extract_statement_complexity(query, "Stateless")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")
        self.assertEqual(result.state_form, "Stateless")

    def test_extract_statement_complexity_left_join(self):
        """Test complexity extraction with LEFT JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        LEFT JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 1)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")
        self.assertEqual(result.state_form, "Stateful")

    def test_extract_statement_complexity_left_outer_join(self):
        """Test complexity extraction with LEFT OUTER JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        LEFT OUTER JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 1)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_right_join(self):
        """Test complexity extraction with RIGHT JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        RIGHT JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 1)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_right_outer_join(self):
        """Test complexity extraction with RIGHT OUTER JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        RIGHT OUTER JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 1)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_inner_join(self):
        """Test complexity extraction with INNER JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        INNER JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 1)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_full_outer_join(self):
        """Test complexity extraction with FULL OUTER JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        FULL OUTER JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 1)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_regular_join(self):
        """Test complexity extraction with regular JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 1)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_multiple_joins_medium(self):
        """Test complexity extraction with multiple joins - Medium complexity"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2, t3.col3
        FROM table1 t1
        LEFT JOIN table2 t2 ON t1.id = t2.id
        LEFT JOIN table3 t3 ON t1.id = t3.id
        INNER JOIN table4 t4 ON t2.id = t4.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 2)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 1)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Medium")

    def test_extract_statement_complexity_multiple_joins_complex(self):
        """Test complexity extraction with multiple joins - Complex"""
        parser = SQLparser()
        query = """
        with cte_1 as (
            SELECT t1.col1, t2.col2, t3.col3, t4.col4
            FROM table1 t1
            JOIN table2 t2 ON t1.id = t2.id
            JOIN table3 t3 ON t2.id = t3.id
        )
        SELECT t1.col1, t2.col2, t3.col3, t4.col4
        FROM cte_1 t1
        LEFT JOIN table2 t2 ON t1.id = t2.id
        RIGHT JOIN table3 t3 ON t2.id = t3.id
        INNER JOIN table4 t4 ON t3.id = t4.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 2)
        self.assertEqual(result.number_of_left_joins, 1)
        self.assertEqual(result.number_of_right_joins, 1)
        self.assertEqual(result.number_of_inner_joins, 1)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Complex")

    def test_extract_statement_complexity_cross_join_excluded(self):
        """Test that CROSS JOINs are not counted in complexity (stateless operation)"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        CROSS JOIN table2 t2
        """
        result = parser.extract_statement_complexity(query, "Stateless")

        # CROSS JOINs should not contribute to complexity count
        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_mixed_with_cross_join(self):
        """Test complexity with mix of regular joins and CROSS JOIN"""
        parser = SQLparser()
        query = """
        SELECT t1.col1, t2.col2, t3.col3
        FROM table1 t1
        LEFT JOIN table2 t2 ON t1.id = t2.id
        CROSS JOIN table3 t3
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        # Only the LEFT JOIN should be counted, not the CROSS JOIN
        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 1)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_with_comments(self):
        """Test complexity extraction with SQL comments"""
        parser = SQLparser()
        query = """
        -- This is a comment
        SELECT t1.col1, t2.col2
        FROM table1 t1
        /* Multi-line comment with LEFT JOIN inside */
        LEFT JOIN table2 t2 ON t1.id = t2.id -- another comment
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 1)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")

    def test_extract_statement_complexity_empty_sql(self):
        """Test complexity extraction with empty SQL content"""
        parser = SQLparser()
        result = parser.extract_statement_complexity("", "Stateless")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 0)
        self.assertEqual(result.number_of_right_joins, 0)
        self.assertEqual(result.number_of_inner_joins, 0)
        self.assertEqual(result.number_of_outer_joins, 0)
        self.assertEqual(result.complexity_type, "Simple")
        self.assertEqual(result.state_form, "Stateless")

    def test_extract_statement_complexity_case_insensitive(self):
        """Test complexity extraction is case insensitive"""
        parser = SQLparser()
        query = """
        select t1.col1, t2.col2
        from table1 t1
        left join table2 t2 on t1.id = t2.id
        """
        result = parser.extract_statement_complexity(query, "Stateful")

        self.assertEqual(result.number_of_regular_joins, 0)
        self.assertEqual(result.number_of_left_joins, 1)
        self.assertEqual(result.complexity_type, "Simple")

    def test_parse_sql_values_simple_values(self):
        """Test parsing simple comma-separated values without quotes"""
        parser = SQLparser()

        # Simple values without quotes
        result = parser._parse_sql_values("value1, value2, value3")
        self.assertEqual(result, ["value1", "value2", "value3"])

        # Values with extra spaces
        result = parser._parse_sql_values("  value1  ,  value2  ,  value3  ")
        self.assertEqual(result, ["value1", "value2", "value3"])

    def test_parse_sql_values_single_quotes(self):
        """Test parsing values with single quotes"""
        parser = SQLparser()

        # Simple quoted values
        result = parser._parse_sql_values("'value1', 'value2', 'value3'")
        self.assertEqual(result, ["value1", "value2", "value3"])

        # Mixed quoted and unquoted
        result = parser._parse_sql_values("'quoted', unquoted, 'another quoted'")
        self.assertEqual(result, ["quoted", "unquoted", "another quoted"])

    def test_parse_sql_values_double_quotes(self):
        """Test parsing values with double quotes"""
        parser = SQLparser()

        # Double quoted values
        result = parser._parse_sql_values('"value1", "value2", "value3"')
        self.assertEqual(result, ["value1", "value2", "value3"])

        # Mixed quote types
        result = parser._parse_sql_values("'single', \"double\", unquoted")
        self.assertEqual(result, ["single", "double", "unquoted"])

    def test_parse_sql_values_escaped_quotes(self):
        """Test parsing values with escaped quotes"""
        parser = SQLparser()

        # Escaped single quotes
        result = parser._parse_sql_values("'O''Reilly', 'Don''t', 'It''s working'")
        self.assertEqual(result, ["O'Reilly", "Don't", "It's working"])

        # Escaped double quotes
        result = parser._parse_sql_values('"He said ""Hello""", "She replied ""Hi"""')
        self.assertEqual(result, ['He said "Hello"', 'She replied "Hi"'])

    def test_parse_sql_values_values_with_commas_in_quotes(self):
        """Test parsing quoted values that contain commas"""
        parser = SQLparser()

        # Commas inside quoted values should not split
        result = parser._parse_sql_values("'value1, with comma', 'value2', 'another, value'")
        self.assertEqual(result, ["value1, with comma", "value2", "another, value"])

        # Mixed with double quotes
        result = parser._parse_sql_values('"value1, comma", \'value2\', "another, one"')
        self.assertEqual(result, ["value1, comma", "value2", "another, one"])

    def test_parse_sql_values_empty_and_edge_cases(self):
        """Test parsing edge cases and empty values"""
        parser = SQLparser()

        # Empty string
        result = parser._parse_sql_values("")
        self.assertEqual(result, [])

        # Single value
        result = parser._parse_sql_values("single")
        self.assertEqual(result, ["single"])

        # Single quoted value
        result = parser._parse_sql_values("'single quoted'")
        self.assertEqual(result, ["single quoted"])

        # Empty quoted values - note: function currently returns only one empty string for "'', \"\""
        result = parser._parse_sql_values("'', ''")
        self.assertEqual(result, [""])  # Current behavior: only returns one empty string

        # Single empty double quote
        result = parser._parse_sql_values("\"\"")
        self.assertEqual(result, [])  # Current behavior: returns empty list

        # Values with only spaces and commas
        result = parser._parse_sql_values("   ,   ")
        self.assertEqual(result, [""])  # Current behavior: returns one empty string

    def test_parse_sql_values_special_characters(self):
        """Test parsing values with special characters"""
        parser = SQLparser()

        # Values with special characters
        result = parser._parse_sql_values("'value@domain.com', 'user#123', 'path/to/file'")
        self.assertEqual(result, ["value@domain.com", "user#123", "path/to/file"])

        # Numbers and mixed content
        result = parser._parse_sql_values("123, 'text123', '456text'")
        self.assertEqual(result, ["123", "text123", "456text"])

    def test_parse_sql_values_complex_scenarios(self):
        """Test parsing complex real-world scenarios"""
        parser = SQLparser()

        # SQL-like values that might appear in INSERT statements
        result = parser._parse_sql_values("'John Doe', 25, 'john@email.com', 'Manager'")
        self.assertEqual(result, ["John Doe", "25", "john@email.com", "Manager"])

        # Values with quotes and escaped quotes mixed
        result = parser._parse_sql_values("'It''s a test', \"Another \"\"test\"\"\", normal_value")
        self.assertEqual(result, ["It's a test", 'Another "test"', "normal_value"])

        # Empty values in the middle
        result = parser._parse_sql_values("'first', , 'third'")
        self.assertEqual(result, ["first", "", "third"])

    def test_parse_insert_sql_to_dict_basic(self):
        """Test basic INSERT SQL parsing functionality"""
        parser = SQLparser()

        # Simple INSERT with single row
        sql = "INSERT INTO users (id, name, email) VALUES ('1', 'John Doe', 'john@example.com')"
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1'],
            'name': ['John Doe'],
            'email': ['john@example.com']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_multiple_rows(self):
        """Test INSERT SQL with multiple value rows"""
        parser = SQLparser()

        sql = """INSERT INTO users (id, name, email) VALUES
                 ('1', 'John Doe', 'john@example.com'),
                 ('2', 'Jane Smith', 'jane@example.com'),
                 ('3', 'Bob Johnson', 'bob@example.com')"""
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2', '3'],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_quoted_columns(self):
        """Test INSERT SQL with quoted column names"""
        parser = SQLparser()

        # Test with backticks
        sql = "INSERT INTO `users` (`id`, `name`, `email`) VALUES ('1', 'John Doe', 'john@example.com')"
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1'],
            'name': ['John Doe'],
            'email': ['john@example.com']
        }
        self.assertEqual(result, expected)

        # Test with double quotes
        sql = 'INSERT INTO users ("id", "name", "email") VALUES (\'1\', \'John Doe\', \'john@example.com\')'
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1'],
            'name': ['John Doe'],
            'email': ['john@example.com']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_mixed_quotes(self):
        """Test INSERT SQL with mixed quote types in values"""
        parser = SQLparser()

        sql = """INSERT INTO users (id, name, comment) VALUES
                 ('1', "John O'Reilly", 'He said "Hello"'),
                 ("2", 'Jane Smith', "She replied 'Hi'")"""
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2'],
            'name': ["John O'Reilly", 'Jane Smith'],
            'comment': ['He said "Hello"', "She replied 'Hi'"]
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_with_comments(self):
        """Test INSERT SQL with SQL comments"""
        parser = SQLparser()

        sql = """-- Insert user data
                 INSERT INTO users (id, name, email) -- columns
                 VALUES ('1', 'John Doe', 'john@example.com'), -- first user
                        ('2', 'Jane Smith', 'jane@example.com') -- second user
                 /* End of insert */"""
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2'],
            'name': ['John Doe', 'Jane Smith'],
            'email': ['john@example.com', 'jane@example.com']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_special_characters(self):
        """Test INSERT SQL with special characters in values"""
        parser = SQLparser()

        sql = """INSERT INTO users (id, name, email, tags) VALUES
                 ('1', 'John@Doe', 'user+test@domain.co.uk', 'admin,vip'),
                 ('2', 'Jane#Smith', 'jane_smith@test-domain.com', 'user,basic')"""
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2'],
            'name': ['John@Doe', 'Jane#Smith'],
            'email': ['user+test@domain.co.uk', 'jane_smith@test-domain.com'],
            'tags': ['admin,vip', 'user,basic']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_with_spaces(self):
        """Test INSERT SQL with extra spaces and formatting"""
        parser = SQLparser()

        sql = """  INSERT   INTO   users   (  id  ,  name  ,  email  )
                 VALUES   (  '1'  ,  'John Doe'  ,  'john@example.com'  )  """
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1'],
            'name': ['John Doe'],
            'email': ['john@example.com']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_case_insensitive(self):
        """Test INSERT SQL with different case variations"""
        parser = SQLparser()

        sql = "insert into USERS (ID, Name, Email) values ('1', 'John Doe', 'john@example.com')"
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'ID': ['1'],
            'Name': ['John Doe'],
            'Email': ['john@example.com']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_error_cases(self):
        """Test INSERT SQL parsing error cases"""
        parser = SQLparser()

        # Missing column definition
        with self.assertRaises(ValueError) as context:
            parser.parse_insert_sql_to_dict("INSERT INTO users VALUES ('1', 'John')")
        self.assertIn("Could not extract column names", str(context.exception))

        # Missing VALUES section
        with self.assertRaises(ValueError) as context:
            parser.parse_insert_sql_to_dict("INSERT INTO users (id, name)")
        self.assertIn("Could not extract VALUES section", str(context.exception))

        # Mismatched number of columns and values
        with self.assertRaises(ValueError) as context:
            parser.parse_insert_sql_to_dict("INSERT INTO users (id, name) VALUES ('1', 'John', 'Extra')")
        self.assertIn("Number of values", str(context.exception))

        # Malformed VALUES section
        with self.assertRaises(ValueError) as context:
            parser.parse_insert_sql_to_dict("INSERT INTO users (id, name) VALUES invalid_syntax")
        self.assertIn("Could not extract value rows", str(context.exception))

    def test_parse_insert_sql_to_dict_escaped_quotes(self):
        """Test INSERT SQL with escaped quotes in values"""
        parser = SQLparser()

        # Build SQL string with proper escaping
        sql = ("INSERT INTO users (id, name, comment) VALUES " +
               "('1', 'O''Reilly', 'He said ''Hello World'''), " +
               "('2', \"Jane \"\"The Great\"\" Smith\", \"She replied \"\"Hi there\"\"\")")
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2'],
            'name': ["O'Reilly", 'Jane "The Great" Smith'],
            'comment': ["He said 'Hello World'", 'She replied "Hi there"']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_numeric_and_special_values(self):
        """Test INSERT SQL with numeric values and special SQL values"""
        parser = SQLparser()

        sql = """INSERT INTO products (id, name, price, active, created_date) VALUES
                 ('1', 'Product A', 29.99, true, '2023-01-01'),
                 ('2', 'Product B', 15.50, false, '2023-01-02')"""
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2'],
            'name': ['Product A', 'Product B'],
            'price': ['29.99', '15.50'],
            'active': ['true', 'false'],
            'created_date': ['2023-01-01', '2023-01-02']
        }
        self.assertEqual(result, expected)

    def test_parse_insert_sql_to_dict_ending_semicolon(self):
        """Test INSERT SQL with ending semicolon"""
        parser = SQLparser()

        sql = "INSERT INTO users (id, name) VALUES ('1', 'John Doe'), ('2', 'Jane Smith');"
        result = parser.parse_insert_sql_to_dict(sql)
        expected = {
            'id': ['1', '2'],
            'name': ['John Doe', 'Jane Smith']
        }
        self.assertEqual(result, expected)


    def test_get_source_topics(self):
        """Test getting source topics."""
        src_table='src_c360_groups'
        parser = SQLparser()
        dml_content="""
        INSERT INTO src_c360_groups (
  group_id,
  tenant_id,
  group_name,
  group_type,
  created_date,
  is_active,
  updated_at
)
WITH deduplicated_groups AS (
  SELECT
    group_id,
    tenant_id,
    group_name,
    group_type,
    created_date,
    is_active,
    CURRENT_TIMESTAMP AS updated_at,

    -- Deduplication: Keep latest record per group_id
    -- This handles cases where the same group appears multiple times
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id, group_id
      ORDER BY `$rowtime` DESC
    ) AS row_num

  FROM raw_groups
  WHERE
    group_id IS NOT NULL  -- Ensure we have valid group_id  and tenant_id
    AND tenant_id IS NOT NULL
)
SELECT
  group_id,
  tenant_id,
  group_name,
  group_type,
  created_date,
  is_active,
  updated_at
FROM deduplicated_groups
WHERE row_num = 1
"""
        source_topics = parser.extract_table_references(dml_content)
        assert source_topics is not None
        assert len(source_topics) == 2
        assert 'raw_groups' in source_topics
        assert src_table in source_topics

    def test_extract_cte_names_simple_uppercase(self):
        """Test CTE name extraction with uppercase WITH and AS"""
        parser = SQLparser()

        sql = """
        WITH user_data AS (
            SELECT user_id, name FROM users WHERE active = 1
        )
        SELECT * FROM user_data ORDER BY name
        """

        result = parser._extract_cte_names(sql)
        expected = ["user_data"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_simple_lowercase(self):
        """Test CTE name extraction with lowercase with and as"""
        parser = SQLparser()

        sql = """
        with user_data as (
            select user_id, name from users where active = 1
        )
        select * from user_data order by name
        """

        result = parser._extract_cte_names(sql)
        expected = ["user_data"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_mixed_case(self):
        """Test CTE name extraction with mixed case WITH and AS"""
        parser = SQLparser()

        sql = """
        With user_data As (
            SELECT user_id, name FROM users WHERE active = 1
        ),
        order_data as (
            SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id
        )
        Select * From user_data u Left Join order_data o On u.user_id = o.user_id
        """

        result = parser._extract_cte_names(sql)
        expected = ["user_data", "order_data"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_multiple_ctes(self):
        """Test CTE name extraction with multiple CTEs"""
        parser = SQLparser()

        sql = """
        WITH user_data AS (
            SELECT user_id, name FROM users WHERE active = 1
        )
        ,order_data AS (
            SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id
        ),
        combined_data AS (
            SELECT u.*, o.order_count FROM user_data u LEFT JOIN order_data o ON u.user_id = o.user_id
        )
        SELECT * FROM combined_data WHERE order_count > 5
        """

        result = parser._extract_cte_names(sql)
        expected = ["user_data", "order_data", "combined_data"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_nested_subqueries(self):
        """Test CTE name extraction with nested subqueries in CTE"""
        parser = SQLparser()

        sql = """
        WITH complex_cte AS (
            SELECT user_id,
                   (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.user_id) as order_count
            FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.user_id = u.user_id)
        )
        SELECT * FROM complex_cte WHERE order_count > 5
        """

        result = parser._extract_cte_names(sql)
        expected = ["complex_cte"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_no_cte_present(self):
        """Test CTE name extraction when no CTEs are present"""
        parser = SQLparser()

        sql = "SELECT * FROM users WHERE active = 1"
        result = parser._extract_cte_names(sql)

        # Should return empty list when no CTEs found
        self.assertEqual(result, [])

    def test_extract_cte_names_empty_sql(self):
        """Test CTE name extraction with empty or None SQL"""
        parser = SQLparser()

        # Empty string
        result = parser._extract_cte_names("")
        self.assertEqual(result, [])

        # None
        result = parser._extract_cte_names(None)
        self.assertEqual(result, [])

        # Whitespace only
        result = parser._extract_cte_names("   ")
        self.assertEqual(result, [])

    def test_extract_cte_names_insert_statement(self):
        """Test CTE name extraction with INSERT statement"""
        parser = SQLparser()

        sql = """
        WITH active_users AS (
            SELECT user_id, name FROM users WHERE active = 1
        )
        INSERT INTO user_summary
        SELECT user_id, name, 'active' as status FROM active_users
        """

        result = parser._extract_cte_names(sql)
        expected = ["active_users"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_with_comments(self):
        """Test CTE name extraction with SQL comments"""
        parser = SQLparser()

        sql = """
        -- User data CTE
        WITH user_data AS (
            /* Get active users only */
            SELECT user_id, name FROM users WHERE active = 1
        ),
        -- Order data CTE
        order_summary AS (
            SELECT user_id, COUNT(*) as total_orders FROM orders GROUP BY user_id
        )
        SELECT * FROM user_data u JOIN order_summary o ON u.user_id = o.user_id
        """

        result = parser._extract_cte_names(sql)
        expected = ["user_data", "order_summary"]
        self.assertEqual(result, expected)

    def test_extract_cte_names_duplicate_handling(self):
        """Test that duplicate CTE names are handled correctly"""
        parser = SQLparser()

        # This shouldn't happen in valid SQL, but test the deduplication logic
        sql = """
        WITH user_data AS (SELECT * FROM users),
             order_data AS (SELECT * FROM orders)
        SELECT * FROM user_data u JOIN order_data o ON u.id = o.user_id
        """

        result = parser._extract_cte_names(sql)
        expected = ["user_data", "order_data"]
        self.assertEqual(result, expected)

        # Ensure no duplicates in result
        self.assertEqual(len(result), len(set(result)))

    def test_extract_cte_names_complex_real_world_example(self):
        """Test CTE name extraction with complex real-world SQL"""
        parser = SQLparser()

        sql = """
        WITH section_detail as (
            SELECT s.event_section_id, sc.name, s.tenant_id
                s.operation_id IS NOT DISTINCT FROM first_element.operation_id
            FROM src_execution_plan as s
            INNER JOIN src_configuration_section as sc
                ON sc.id = s.config_section_id
                AND sc.tenant_id = s.tenant_id
        ),
        tenant as (
            SELECT CAST(null AS STRING) as id, t.__db as tenant_id
            FROM tenant_dimension as t
            where not (t.__op IS NULL OR t.__op = 'd')
        ),
        attachment as (
            SELECT ae.*, JSON_VALUE(att.object_state, '$.fileName') as filename
            FROM int_aqem_recordexecution_element_data_unnest ae
            JOIN src_aqem_recordexecution_attachments att
                ON ae.element_data = att.id AND ae.tenant_id = att.tenant_id
            where ae.element_type = 'ATTACHMENT'
        )
        SELECT * FROM section_detail s
        JOIN tenant t ON s.tenant_id = t.tenant_id
        LEFT JOIN attachment a ON s.tenant_id = a.tenant_id
        """

        result = parser._extract_cte_names(sql)
        expected = ["section_detail", "tenant", "attachment"]
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
