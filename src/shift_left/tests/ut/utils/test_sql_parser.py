import unittest
import os
import pathlib
from shift_left.core.utils.sql_parser import (
    SQLparser
)


class TestFileSearch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")

    def test_get_table_from_select(self):
        parser = SQLparser()
        query="""
        SELECT * FROM table1
        """
        rep=parser.extract_table_references(query)
        assert rep
        assert "table1" in rep

    def test_get_tables_from_join(self):
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

    def test_get_tables_from_inner_join(self):
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

    def test_get_tables_from_right_join(self):
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
    
    def test_get_tables_without_ctes(self):
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
    

    def test_get_tables_without_ctes(self):
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

    def test_extract_table_name_from_insert(self):
        parser = SQLparser()
        query="INSERT INTO mytablename\nSELECT a,b,c\nFROM src_table"
        rep=parser.extract_table_references(query)
        assert rep
        assert "src_table" in rep
        print(rep)

    def test_sql_content(self):
        fname = pathlib.Path(__file__).parent / "./tmp/dml.test_cpx_1.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            parser = SQLparser()
            referenced_table_names = parser.extract_table_references(sql_content)
            print(referenced_table_names)

    def test_sql_content_order(self):
        fname = os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts/dml.fct_order.sql"
        with open(fname, "r") as f:
            sql_content = f.read()
            parser = SQLparser()
            referenced_table_names = parser.extract_table_references(sql_content)
            assert len(referenced_table_names) == 3


if __name__ == '__main__':
    unittest.main()