"""

The processing of dbt sql statements for defining sources adopt a different approach than sink tables.

Processes per application, and generates the sql in the existing pipelines. 
So this script needs to be created after the pipeline folder structure is created from the sink, as 
there is one pipeline per sink table.

"""
import os, argparse, sys
from pathlib import Path
from flink_sql_code_agent_lg import translate_to_flink_sqls
from jinja2 import Environment, FileSystemLoader
import re
from flink_sql_code_agent_lg import translate_to_flink_sqls
from find_path_for_table import build_all_file_inventory, search_table_in_inventory, list_sql_files
from clean_sql import process_ddl_file

TMPL_FOLDER="./templates"
TABLES_TO_PROCESS="./process_out/tables_to_process.txt"
TABLES_DONE="./process_out/tables_done.txt"
CREATE_TABLE_TMPL="create_table_squeleton.jinja"
#DML_DEDUP_TMPL="dedup_dml_squeleton.jinja"
DML_DEDUP_TMPL="dml_to_upsert.jinja"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate flink project for a given dbt starting code - different options'
)
parser.add_argument('-f', '--folder_path', required=False, help="name of the folder including the sink tables, could be a .sql file only")
parser.add_argument('-o', '--pipeline_folder_path', required=True, help="name of the folder output of the pipelines")
parser.add_argument('-t', '--table_name', required=False, help="name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file.")
parser.add_argument('-ld', action=argparse.BooleanOptionalAction, default= False, help="For the given file or folder list for each table their table dependancies")
parser.add_argument('-pd', action=argparse.BooleanOptionalAction, default= False, help="For the given file, process also its dependencies so a full graph is created NOT SUPPORTED")
# Attention pd is for taking a sink and process up to all sources.

# --- utilities functions
def create_folder_if_not_exist(new_path):
    if not os.path.exists(new_path):
        os.makedirs(new_path)




def create_ddl_squeleton(table_name:str, target_folder: str):
    """
    Create in the target folder a ddl sql squeleton file for the given table name

    :param table_name: The table name for the ddl sql to create
    :param target_folder: The folder where to create the ddl statement as sql file
    :return: create a file in the target folder
    """
    file_name=f"{target_folder}/ddl.{table_name}.sql" 
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{CREATE_TABLE_TMPL}")

    context = {
        'table_name': table_name,
    }
    rendered_sql = sql_template.render(context)
    print(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)

def create_dedup_dml_squeleton(table_name:str, target_folder: str):
    """
    Create in the target folder a dml sql squeleton file for the given table name

    :param table_name: The table name for the dml sql to create
    :param target_folder: The folder where to create the dml statement as sql file
    :return: create a file in the target folder
    """
    file_name=f"{target_folder}/dml.{table_name}.sql" 
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{DML_DEDUP_TMPL}")

    context = {
        'table_name': table_name,
    }
    rendered_sql = sql_template.render(context)
    print(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)

def create_makefile(table_name: str, ddl_folder: str, dml_folder: str, out_dir: str):
    """
    Create a makefile to help deploy Flink statements for the given table name
    When the dml folder is called dedup the ddl should include a table name that is `_raw` suffix.
    """
    env = Environment(loader=FileSystemLoader('.'))
    if dml_folder == "dedups":
        makefile_template = env.get_template(f"{TMPL_FOLDER}/makefile_src_dedup_tmpl.jinja")
    else:
        makefile_template = env.get_template(f"{TMPL_FOLDER}/makefile_ddl_dml_tmpl.jinja")
    
    context = {
        'table_name': table_name,
        'statement_name': table_name.replace("_","-"),
        'ddl_folder': ddl_folder,
        'dml_folder': dml_folder
    }
    rendered_makefile = makefile_template.render(context)
    # Write the rendered Makefile to a file
    with open(out_dir + '/Makefile', 'w') as f:
        f.write(rendered_makefile)


def extract_table_name(src_file_name: str, prefix_to_remove: str = None) -> str:
    the_path= Path(src_file_name)

    table_name = the_path.stem
    if prefix_to_remove:
        table_name = table_name.replace(prefix_to_remove,"")
    print(f"Process the table: {table_name}")
    return table_name

def parse_sql(table_name,sql_content):
    """
    Extract the sql dependant table
    """
    #regex = r'{{\s*ref\([\'"]([^\']+)[\'"]\)\s*}}'
    #regex= r'{{\s*ref\(["\']([^"\']+)"\')\s*}}'
    regex=r'ref\([\'"]([^\'"]+)[\'"]\)'
    matches = re.findall(regex, sql_content)
    return matches

def get_dependencies(table_name, dbt_script_content) -> list[str]:
    """
    For a given table and dbt script content, get the dependent tables using the dbt { ref: } template
    """
    dependencies = []
    dependency_names = parse_sql(table_name,dbt_script_content)

    if (len(dependency_names) > 0):
        print("Dependencies found:")
        for dependency in dependency_names:
            print(f"  - depends on : {dependency}")
            dependencies.append(dependency)
    else:
        print("  - No dependency")
    return dependencies

def save_one_file(fname: str, content: str):
    print(f"Write {fname}")
    with open(fname,"w") as f:
        f.write(content)

def save_dml_ddl(content_path: str, table_name: str, dml: str, ddl: str):
    """
    creates two files, prefixed by "ddl." and "dml." from the dml and ddl SQL statements
    """
    save_one_file(f"{content_path}/sql-scripts/dml.{table_name}.sql",dml)
    ddl_fn=f"{content_path}/sql-scripts/ddl.{table_name}.sql"
    save_one_file(ddl_fn,ddl)
    process_ddl_file(f"{content_path}/sql-scripts/",ddl_fn)



# ----- more specific functions
def process_src_sql_file(src_file_name: str, source_target_path: str):
    """
    Transform the source file content to the new Flink SQL format within the source_target_path.
    It creates a tests folder for the ddl source raw table creation. This is done in the tests folder
    as in staging or production environment the CDC Debezium tool will create those topics so tables.
    The dedups folder includes a dml to dedup records from the raw table records.

    The source file is added to the table to process tracking file

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file

    """
    table_name = extract_table_name(src_file_name,"src_")
    table_folder=f"{source_target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/dedups")
    create_folder_if_not_exist(f"{table_folder}/tests")
    create_ddl_squeleton(table_name,f"{table_folder}/tests")
    create_dedup_dml_squeleton(table_name,f"{table_folder}/dedups")
    create_makefile(table_name, "tests", "dedups", table_folder)
    merge_items_in_processing_file([table_name], TABLES_TO_PROCESS)


def process_fact_dim_sql_file(src_file_name: str, source_target_path: str, walk_parent: bool = False):
    """
    Transform stage, fact or dimension sql file to Flink SQL. 
    The folder created are <table_name>/sql_scripts and <table_name>/tests + a makefile to facilitate Confluent cloud deployment.

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file
    :param walk_parent: Assess if it needs to process the dependencies
    """
    table_name = extract_table_name(src_file_name)
    table_folder=f"{source_target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/sql-scripts")
    create_folder_if_not_exist(f"{table_folder}/tests")
    create_makefile(table_name, "sql-scripts", "sql-scripts", table_folder)
    with open(src_file_name) as f:
        sql_content= f.read()
        l=get_dependencies(table_name, sql_content)
        merge_items_in_processing_file(l,TABLES_TO_PROCESS)
        dml, ddl = translate_to_flink_sqls(table_name, sql_content)
        save_dml_ddl(table_folder, table_name, dml, ddl)
        # not sure we want that: merge_items_in_processing_file([table_name], TABLES_DONE)
    if walk_parent:
        print("NOT IMPLEMENTED")



def create_target_folder(target_root_folder: str) -> str:
    """
    For the given target root folder, creates the structure for the 
    sources subfolder 
    """
    sources_path=f"{target_root_folder}"
    create_folder_if_not_exist(sources_path)
    return sources_path


def select_src_sql_file_processing(sql_file_path: str, source_target_path: str, walk_parent: bool = False):
    """
    the source tables have a different processing, so this is the routing function
    """
    print("\n\n-----------------------------")
    print("\t PROCESS file: {sql_file_path}")
    if sql_file_path.find("source") > 0:
        process_src_sql_file(sql_file_path, source_target_path)
    else:
        process_fact_dim_sql_file(sql_file_path, source_target_path, walk_parent)

def process_files_in_folder(args):
    """
    List all sql files within the source folder, for each of them
    prepare the target Flink ddls and makefile
    """
    sql_files = list_sql_files(args.folder_path)
    source_target_path= create_target_folder(args.pipeline_folder_path)
    for sql_file in sql_files:
        select_src_sql_file_processing(sql_file, source_target_path, args.pd)
        print("\n\n----------")


def process_one_file(src_file: str, target_folder: str, process_dependency: bool = False):
    """ Process one sql file """
    sources_path=create_target_folder(target_folder)
    select_src_sql_file_processing(src_file, sources_path, process_dependency)


def merge_items_in_processing_file(dependencies: list[str], persistent_file):
    """
    using the existing persistenc_file, save the dependency in the list of dependencies if not present in the file
    The file contains as a first column the name of the dependencies
    """
    existing_lines = set()
    with open(persistent_file, 'r') as file:
        for line in file:
            if line.find(",") > 0:
                element=line.split(",")[0]
            else:
                element=line.strip()
            existing_lines.add(element)
    elements_to_add = [elem for elem in dependencies if elem not in existing_lines]

    # Append new unique elements
    if elements_to_add:
        with open(persistent_file, 'a') as file:
            for element in elements_to_add:
                file.write(f"{element}\n")

def list_dependencies(file_or_folder: str, persist_dependencies: bool = False):
    """
    List the dependencies for the given file, or all the dependencies of all tables in the given folder
    """
    if file_or_folder.endswith(".sql"):
        table_name = extract_table_name(file_or_folder)
        with open(file_or_folder) as f:
            sql_content= f.read()
            l=get_dependencies(table_name, sql_content)
            if persist_dependencies:
                merge_items_in_processing_file(l,TABLES_TO_PROCESS)
            return l
    else:
        # loop over the files in the folder
        for file in list_sql_files(file_or_folder):
            list_dependencies(file)

def proces_from_table_name(args):
    """
    Load matching sql file given the table name as input.
    This method may be useful when we get the table name from the dependencies list of another table.

    :param: the program argument with the table name is populated
    """
    all_files= build_all_file_inventory()
    matching_sql_file=search_table_in_inventory(args.table_name,all_files)
    if matching_sql_file:
        print(f"Start processing the table: {args.table_name} from the dbt file: {matching_sql_file}")
        process_one_file(matching_sql_file, args.pipeline_folder_path, args.pd)
    else:
        print("Matching sql file not found !")
    
if __name__ == "__main__":
    args = parser.parse_args()
    if args.ld:
        list_dependencies(args.folder_path, True)
        sys.exit()
    if args.folder_path:
        if args.folder_path.endswith(".sql"):
            process_one_file(args.folder_path, args.pipeline_folder_path, args.pd)
        else:    
            process_files_in_folder(args)
    elif args.table_name:
        proces_from_table_name(args)

    print("\n\nDone !")
    