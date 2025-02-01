import os, argparse
from functools import lru_cache
from process_src_tables import list_sql_files

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
    last_name = file_path.split("/")[-1]
    return last_name

def process_each_sql_file(table_name: str, file_path: str) -> str:
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
    output=f"# {table_name} is referenced in "
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
       found=process_each_sql_file(args.table_name, fn)
       if found:
           results.add(found)
    report = build_report(args.table_name,results)
    if args.save_file_name:
        with open(args.save_file_name, "w") as f:
            f.write(report)
            print(f"\n Save result to {args.save_file_name}")

    print(report)

if __name__ == "__main__":
    main()