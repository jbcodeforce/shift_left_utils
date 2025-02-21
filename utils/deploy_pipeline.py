"""
A tool to read a pipeline dependencies and process each ddl and dml deploment in the expected order using the Flink Gateway REST API.
"""
from pydantic import BaseModel, ValidationError, ConfigDict
from typing import List, Optional, Any
import os, argparse
from pathlib import Path
import json
from sql_parser import SQLparser

PIPELINE_JSON_FILE_NAME="pipeline_definition.json"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file_name', required=False, help="File name for the pipeline metadata in json format.")
parser.add_argument('-t', '--table_name', required=False, help="Table name to build a pipeline or to process a pipeline on.")


class FlinkStatementHierarchy(BaseModel):
    name: str
    type: str
    ddl_ref: str
    dml_ref: str
    parents: Optional[List[Any]]
    children: Optional[List[str]]

def read_pipeline_metadata(file_name: str) -> FlinkStatementHierarchy:
    pass


def test_data() -> FlinkStatementHierarchy:
    root: FlinkStatementHierarchy = FlinkStatementHierarchy.model_validate({"name": "fact_table", "type": "fact", "ddl_ref": "facts/fact_table/sql-scripts/ddl.fact_table.sql", "dml_ref": "facts/fact_table/sql-scripts/dml.fact_table.sql", "parents": [], "children": None})
    parent_1=FlinkStatementHierarchy.model_validate({"name": "int_table_1", "type": "intermediate", "ddl_ref": "intermediates/int_table/sql-scripts/ddl.int_table.sql", "dml_ref": "intermediates/int_table/sql-scripts/dml.int_table.sql", "parents": [], "children": []})
    parent_1.children.append(root.name)
    parent_2=FlinkStatementHierarchy.model_validate({"name": "int_table_2", "type": "intermediate", "ddl_ref": "intermediates/int_table/sql-scripts/ddl.int_table.sql", "dml_ref": "intermediates/int_table/sql-scripts/dml.int_table.sql", "parents": [], "children": []})
    parent_2.children.append(root.name)
    src_parent_1=FlinkStatementHierarchy.model_validate({"name": "src_table_1", "type": "source", "ddl_ref": "sources/src_table/sql-scripts/ddl.src_table_1.sql", "dml_ref": "sources/arc_table/sql-scripts/dml.src_table_1.sql", "parents": None, "children": []})
    src_parent_1.children.append(parent_1.name)
    src_parent_2=FlinkStatementHierarchy.model_validate({"name": "src_table_2", "type": "source", "ddl_ref": "sources/src_table/sql-scripts/ddl.src_table_2.sql", "dml_ref": "sources/arc_table/sql-scripts/dml.src_table_2.sql", "parents": None, "children": []})
    src_parent_2.children.append(parent_2.name)
    parent_1.parents.append(src_parent_1)
    parent_2.parents.append(src_parent_2)

    root.parents.append(parent_1)
    root.parents.append(parent_2)
    return root

def _is_ddl_exists(folder_path: str) -> str:
    """
    Return the ddl file name if it exists in the given folder
    """
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.startswith('ddl'):
                return file
    return None

def build_hierarchy(dml_file_name: str, table_name: str, level:str, parents: List[str]) -> FlinkStatementHierarchy:
    """
    Return 
    """
    directory = os.path.dirname(dml_file_name)
    ddl_file_name=_is_ddl_exists(directory)
    root = FlinkStatementHierarchy.model_validate({"name": table_name, 
                                          "type": level, 
                                          "ddl_ref": ddl_file_name, 
                                          "dml_ref":  dml_file_name, 
                                          "parents": [], 
                                          "children": None})
    for parent in parents:
        parent_1=FlinkStatementHierarchy.model_validate({"name": parent, 
                                                "type": "intermediate", 
                                                "ddl_ref": "intermediates/int_table/sql-scripts/ddl.int_table.sql", 
                                                "dml_ref": "intermediates/int_table/sql-scripts/dml.int_table.sql", 
                                                "parents": [], 
                                                "children": [root.name]})
        root.parents.append(parent_1)
    return root

def get_dependent_tables(file_name: str) -> List[str]:
    """
    From the given sql file name, use sql parser to get the tables used by this sql
    content, and return the list of table name
    """
    try:
        with open(file_name) as f:
            sql_content= f.read()
            parser = SQLparser()
            table_name=parser.extract_table_name_from_insert(sql_content)
            dependencies = []
            dependency_names = parser.extract_table_references(sql_content)
            if (len(dependency_names) > 0):
                for dependency in dependency_names:
                    print(f"- depends on : {dependency}")
                    dependencies.append(dependency)
            return table_name, dependencies
    except Exception as e:
        print(e)
        return "", []


def build_pipeline_definition_from_table(file_name: str):
    """
    From the DML file name build a pipeline definition taking into account the parents
    of the given table. The file include INSERT INTO and then one to many FROM or JOIN statements.
    Those FROM and JOIN help getting the parent tables needed by the current table.
    The outcome is a hierarchy of parents with the referentce on how to build them, at one level

    """
    table_name, parents=get_dependent_tables(file_name)
    hierarchy: FlinkStatementHierarchy=build_hierarchy(file_name, table_name, 'facts', parents)
    #pname = _get_path_to_pipeline_file(file_name)
    #with open( pname, "w") as f:
    #    f.write(hierarchy.model_dump_json())
    print(hierarchy.model_dump_json())



def _get_path_to_pipeline_file(file_name: str) -> str:
    directory = os.path.dirname(file_name)
    if "sql-scripts" in directory:
        pname = f"{directory}/../{PIPELINE_JSON_FILE_NAME}"
    else:
        pname = f"{directory}/{PIPELINE_JSON_FILE_NAME}"
    return pname   
    
def assess_pipeline_definition_exists(file_name: str) -> bool:
    """
    From the given sql file path that includes the table we want to have the pipeline
    extract the file_path, and looks at the metadata file
    """
    try:
        pname = _get_path_to_pipeline_file(file_name)
        if os.path.exists(pname):
            try:
                with open(pname, 'r') as f:
                    json.load(f)
                return True
            except json.JSONDecodeError:
                return False
        else:
            return False
    except Exception as e:
        return False

if __name__ == "__main__":
    args = parser.parse_args()
    if assess_pipeline_definition_exists(args.file_name):
        print(f"Found {PIPELINE_JSON_FILE_NAME}")
    else:
         print(f"{PIPELINE_JSON_FILE_NAME} not found")
         build_pipeline_definition_from_table(args.file_name)
    #read_pipeline_metadata(args.file_name)
    
