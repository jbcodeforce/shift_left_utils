import os, argparse
from functools import lru_cache
from process_src_tables import list_sql_files

"""
Find the consumers of a given table by searching the table name used in the FROM within
Flink SQL statement.
The tool search from a root folder given in parameter.

For example to search for the existing pipelines the call will be

python find_table_user.py -r ../pipelines/ -t mc_users -s list_tables.txt
"""


parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the flink sqls which are referencing the named table'
)

parser.add_argument('-r', '--root_folder', required=True, help="name of the root folder which includes all the flink sql files")
parser.add_argument('-t', '--table_name', required=True, help="name of the table to search the flink users")
parser.add_argument('-s', '--save_file_name', required=False, help="name of the file to save the tracking content")

@lru_cache
def build_all_file_inventory(root_folder: str) -> set[str]:
    """
    There are two folders to keep the flink sql, the final pipelines, and the staging
    where flink sql are under development.
    """
    print(root_folder)
    if root_folder:
        file_paths=list_sql_files(f"{root_folder}")
        return file_paths
    else:
        return set()

def extract_table_name(file_path: str) -> str:
    """
    Given the file path that includes the name of the table return the last element of the path
    ehich should be a table_name
    """
    last_name = file_path.split("/")[-1]
    return last_name

def search_topic_for_table(table_name: str, file_path: str) -> str:
    """
    Search if the table name is referenced in the file content specified by the file_path
    """
    if table_name in file_path:
        # do not want to reference itself
        return None
    else:
        with open(file_path, "r") as f:
            for line in f:
                if table_name in line:
                    return extract_table_name(file_path)
    return None

def build_report(table_name: str, results):
    """
    Create a human readable report.
    """
    output=f"# {table_name} is referenced in {len(results)} Flink SQL statements:"
    if len(results) == 0:
        output+="\n\t no table ... yet"
    else:
        for t in results:
            output+=f"\n\t{t}"
    return output

def main():
    args = parser.parse_args()
    all_files= build_all_file_inventory(args.root_folder)
    results = set()
    for fn in all_files:
       found=search_topic_for_table(args.table_name, fn)
       if found:
           results.add(f"table: {found}, file_ref: {fn}")
    report = build_report(args.table_name,results)
    if args.save_file_name:
        with open(args.save_file_name, "w") as f:
            f.write(report)
            print(f"\n Save result to {args.save_file_name}")

    print(report)

if __name__ == "__main__":
    main()