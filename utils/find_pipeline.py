from process_src_tables import list_dependencies
from find_path_for_table import build_all_file_inventory, search_table_in_inventory
import os, argparse
from collections import deque
from pathlib import Path

files_to_process= deque()
dependency_list = set()

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file_name', required=True, help="name of the file of the sink table")
parser.add_argument('-s', '--save_file_name', required=True, help="name of the file to save the tracking content")

def process_files_from_queue(files_to_process, all_files):
    """
    For each file in the queue get the parents (dependencies) of the table
    declare in the file. Get the matching file name in the dbt project of each of those parent table,
    when found at this file to the queue so this code can build the dependency pipeline
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

def generate_tracking_output(file_name: str, dep_list) -> str:
    the_path= Path(file_name)

    table_name = the_path.stem
    output=f"""## Tracking the pipeline implementation for table: {table_name}
    
    -- Processed file: {file_name}
    --- DDL of the table -> NOT_TESTED | OK
    --- DML of the table -> NOT_TESTED | OK

    --- Final result is a list of tables in the pipeline:
    """
    output+="\n"
    output+="\n".join(f"NOT_TESTED || OK | Table: {str(d[0])},\tSrc dbt: {str(d[1])}" for d in dep_list)
    output+="\n\n## Data\n"
    output+="Created with tool and updated to make the final join working on the merge conditions:\n"
    return output

if __name__ == "__main__":
    args = parser.parse_args()
    all_files= build_all_file_inventory()
    files_to_process.append(args.file_name)
    dl=process_files_from_queue(files_to_process, all_files)
    output=generate_tracking_output(args.file_name, dl)
    if args.save_file_name:
        with open(args.save_file_name, "w") as f:
            f.write(output)
            print(f"\n Saved to {args.save_file_name}")
    print(output)

        