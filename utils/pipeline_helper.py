
from functools import lru_cache
import os, argparse
from collections import deque
from pathlib import Path
from create_sink_structure import extract_table_name
from sql_parser import SQLparser

"""
Provides a set of functions to search dependencies from one sink table up to the sources from the dbt project
or from the migrated project.
It reads the from logic for each sql file and build a queue of tables to search.
"""

files_to_process= deque()   # keep a list file to process, as when parsing a sql we can find a lot of dependencies
dependency_list = set()


"""
As a standalone tool it defines some arguments
"""
parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-i', '--inventory', required=False, help="name of the folder used to get all the context for the search, the inventory of table")
parser.add_argument('-f', '--file_name', required=True, help="name of the file of the sink table")
parser.add_argument('-s', '--save_file_name', required=False, help="name of the file to save the tracking content")

def list_sql_files(folder_path: str) -> set[str]:
    """
    Given the folder path, list the sql statements and use the name of the file as table name
    return the list of files and table name

    :param folder_path: path to the folder which includes n sql files
    :return: Set of complete file path for the sql file in the folder
    """
    sql_files = set()
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql') and file.startswith("dml"):
                sql_files.add(os.path.join(root, file))   
    return sql_files

@lru_cache
def build_all_file_inventory(src_path= os.getenv("SRC_FOLDER","../dbt-src/models")) -> set[str]:
    """
    Given the path where all the sql files are persisted, build an in-memory list of paths.
    The list of folders is for a classical dbt project, but works for Flink project too
    :return: a set of sql file paths
    """
    file_paths=list_sql_files(f"{src_path}/intermediates")
    file_paths.update(list_sql_files(f"{src_path}/dimensions"))
    file_paths.update(list_sql_files(f"{src_path}/stage"))
    file_paths.update(list_sql_files(f"{src_path}/facts"))
    file_paths.update(list_sql_files(f"{src_path}/sources"))
    file_paths.update(list_sql_files(f"{src_path}/dedups"))
    return file_paths

def search_table_in_inventory(table_name: str, inventory: set[str]) -> str | None:
    """
    :return: the path to access the sql file for the matching table: the filename has to include the table name
    """
    for apath in inventory:
        if table_name+'.' in apath:
            return apath
    else:
        return None
    

def get_dependencies(table_name, dbt_script_content) -> list[str]:
    """
    For a given table and dbt script content, get the dependent tables using the dbt { ref: } template
    """
    parser = SQLparser()
    dependencies = []
    dependency_names = parser.extract_table_references(dbt_script_content)

    if (len(dependency_names) > 0):
        print("Dependencies found:")
        for dependency in dependency_names:
            print(f"- depends on : {dependency}")
            dependencies.append(dependency)
    else:
        print("  - No dependency")
    return dependencies

def list_dependencies(file_or_folder: str, persist_dependencies: bool = False):
    """
    List the dependencies for the given file, or all the dependencies of all tables in the given folder
    """
    if file_or_folder.endswith(".sql"):
        table_name = extract_table_name(file_or_folder)
        with open(file_or_folder) as f:
            sql_content= f.read()
            l=get_dependencies(table_name, sql_content)
            return l
    else:
        # loop over the files in the folder
        for file in list_sql_files(file_or_folder):
            list_dependencies(file,persist_dependencies)

def process_files_from_queue(files_to_process, all_files):
    """
    For each file in the queue get the parents (dependencies) of the table declared in the file. 
    Get the matching file name in the dbt project of each of those parent table,
    when found add the filename to the queue so this code can build the dependency pipeline.

    :parameter: files to process
    :parameter: all_files: an inventory of all sql file in a project.
    """
    if (len(files_to_process) > 0):
        fn = files_to_process.popleft()
        #print(f"\n\n-- Process file: {fn}")
        current_dependencies=set(list_dependencies(fn))
        for dep in current_dependencies:
            matching_sql_file=search_table_in_inventory(dep, all_files)
            if matching_sql_file:
                dependency_list.add((dep, matching_sql_file))
                files_to_process.append(matching_sql_file)
            else:
                dependency_list.add((dep,None))
        return process_files_from_queue(files_to_process, all_files)
    else:
        return dependency_list

def generic_search_in_processed_tables(table_name: str, root_folder: str) -> bool:
    """
    It is assume that the table_name will a folder name in the tree from the root folder
    """
    for root, dirs, files in os.walk(root_folder):
        for dir in dirs:
            if table_name == dir:
                return True
    else:
        return False
    
    
def search_table_in_processed_tables(table_name: str) -> bool:
    """
    Search in pipeline  and staging folders
    """
    pipeline_path=os.getenv("PIPELINE_FOLDER","../pipelines")
    if not generic_search_in_processed_tables(table_name,pipeline_path):
        staging_path=os.getenv("STAGING","../staging")
        return generic_search_in_processed_tables(table_name,staging_path)
    else:
      return True
    
def generate_tracking_output(file_name: str, dep_list) -> str:
    the_path= Path(file_name)

    table_name = the_path.stem
    output=f"""## Tracking the pipeline implementation for table: {table_name}
    
    -- Processed file: {file_name}
    --- Final result is a list of tables in the pipeline:
    """
    output+="\n"
    output+="\n".join(f"NOT_TESTED || OK | Table: {str(d[0])},\tSrc: {str(d[1])}" for d in dep_list)
    output+="\n\n## Data\n"
    output+="Created with tool and updated to make the final join working on the merge conditions:\n"
    return output

if __name__ == "__main__":
    """
    Build the inventory of source files, then search the given table name or
    the tables referenced in the sql file specified in parameter
    """
    args = parser.parse_args()
    if args.inventory:
        all_files= build_all_file_inventory(args.inventory)
    else:
        all_files= build_all_file_inventory()
    if args.file_name:
        files_to_process.append(args.file_name)
        dependencies=process_files_from_queue(files_to_process, all_files)
        output=generate_tracking_output(args.file_name, dependencies)
    
        if args.save_file_name:
            with open(args.save_file_name, "w") as f:
                f.write(output)
                print(f"\n Save result to {args.save_file_name}")
        print(output)

        