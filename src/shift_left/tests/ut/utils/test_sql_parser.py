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
        assert "Stateless" in parser.extract_upgrade_mode(query)

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
        assert "Stateful" in parser.extract_upgrade_mode(query)

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
        assert "Stateful" in parser.extract_upgrade_mode(query)

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
        assert "Stateful" in parser.extract_upgrade_mode(query)

        
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
        assert "Stateful" in parser.extract_upgrade_mode(query)
    

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
        assert "Stateless" in parser.extract_upgrade_mode(query)

    def test_upgrade_mode_extract_table_name_from_insert(self):
        parser = SQLparser()
        query="INSERT INTO mytablename\nSELECT a,b,c\nFROM src_table"
        rep=parser.extract_table_references(query)
        assert rep
        assert "src_table" in rep
        print(rep)
        assert "Stateless" in parser.extract_upgrade_mode(query)


    def test_upgrade_mode_sql_content_order(self):
        fname = os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            parser = SQLparser()
            referenced_table_names = parser.extract_table_references(sql_content)
            assert len(referenced_table_names) == 3
            assert "Stateful" in parser.extract_upgrade_mode(sql_content)

    def test_upgrade_mode_stateless_from_dml_ref(self):
        parser = SQLparser()
        fname = os.getenv("PIPELINES") + "/sources/p2/src_a/sql-scripts/dml.src_p2_a.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            upgrade_mode = parser.extract_upgrade_mode(sql_content)
            assert "Stateless"  == upgrade_mode

    def test_upgrade_mode_cross_join_unnest(self):
        parser = SQLparser()
        query="""
        SELECT order_id, product_name
        FROM Orders 
            CROSS JOIN UNNEST(product_names) AS t(product_name)
        """
        assert "Stateless" in parser.extract_upgrade_mode(query)

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
        sql_statement_multiple = 'PRIMARY KEY(id, name) NOT ENFORCED'
        match_multiple = re.search(r'PRIMARY KEY\((.*?)\)', sql_statement_multiple, re.IGNORECASE)

        if match_multiple:
            column_names_str_multiple = match_multiple.group(1)
            column_names_multiple = [name.strip() for name in column_names_str_multiple.split(',')]
            print(column_names_multiple)
        else:
            print("No primary key found in the statement.")
        sql_statement = 'id STRING NOT NULL, tenant_id STRING NOT NULL, status STRING, name STRING, `type` STRING, created_by STRING, created_date BIGINT, last_modified_by STRING, PRIMARY KEY(id, tenant_id'
        result = re.sub(r'^.*PRIMARY KEY\(', '', sql_statement, flags=re.IGNORECASE)
        print(result)

    def test_extract_table_name_from_insert_into_statement(self):   
        parser = SQLparser()
        query="""INSERT INTO aqem_dim_event_element
        WITH
            section_detail as (
                SELECT s.event_section_id, sc.name, s.tenant_id
                FROM `src_aqem_recordexecution_execution_plan_section` as s
                INNER JOIN
                    `src_aqem_recordconfiguration_section` as sc
                    ON sc.id = s.config_section_id
                    AND sc.tenant_id = s.tenant_id
            ),
            tenant as (
                SELECT CAST(null AS STRING) as id, t.__db as tenant_id
                FROM `stage_tenant_dimension` as t
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
        query="""INSERT INTO aqem_dim_event_element
        WITH
            section_detail as (
                SELECT s.event_section_id, sc.name, s.tenant_id
                FROM `src_aqem_recordexecution_execution_plan_section` as s
                INNER JOIN
                    `src_aqem_recordconfiguration_section` as sc
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
            type STRING,
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
        print(columns)

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
        
if __name__ == '__main__':
    unittest.main()