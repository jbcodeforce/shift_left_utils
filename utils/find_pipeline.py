from process_src_tables import list_dependencies
from find_path_for_table import build_all_file_inventory, search_table_in_inventory
import os, argparse
from collections import deque

files_to_process= deque()
dependency_list = set()

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file_name', required=True, help="name of the file of the sink table")

def process_files_from_queue():
    """
    For each file in the queue get the parents (dependencies) of the table
    declare in the file. Get the matching file name in the dbt project of each of those parent table,
    when found at this file to the queue so this code can build the dependency pipeline
    """
    if (len(files_to_process) > 0):
        fn = files_to_process.popleft()
        print(f"\n\n-- Process file: {fn}")
        current_dependencies=set(list_dependencies(fn))
        for dep in current_dependencies:
            matching_sql_file=search_table_in_inventory(dep,all_files)
            if matching_sql_file:
                dependency_list.add((dep,matching_sql_file))
                files_to_process.append(matching_sql_file)
            else:
                dependency_list.add((dep,None))
        return process_files_from_queue()
    else:
        return dependency_list

if __name__ == "__main__":
    args = parser.parse_args()
    all_files= build_all_file_inventory()
    files_to_process.append(args.file_name)
    dl=process_files_from_queue()
    print("\n--- Final result is a list of tables in the pipeline: ")
    for d in dl:
        print(d)

        