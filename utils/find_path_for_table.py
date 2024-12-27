import os
TABLES_TO_PROCESS="./process_out/tables_to_process.txt"

"""
Program to get the matching file name for a given table name.
The source inventory is the dbt project.

"""
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
            if file.endswith('.sql'):
                sql_files.add(os.path.join(root, file))   
    return sql_files

def build_all_file_inventory() -> set[str]:
    src_path=os.getenv("SRC_FOLDER","../dbt-src")
    file_paths=list_sql_files(f"{src_path}/models/intermediates")
    file_paths.update(list_sql_files(f"{src_path}/models/dimensions"))
    file_paths.update(list_sql_files(f"{src_path}/models/stage"))
    file_paths.update(list_sql_files(f"{src_path}/models/facts"))
    file_paths.update(list_sql_files(f"{src_path}/models/sources"))
    file_paths.update(list_sql_files(f"{src_path}/models/dedups"))
    return file_paths

def search_table_in_inventory(table_name: str, inventory: set[str]) -> str | None:
    """
    :return: the path to access the sql file for the matching table: the filename has to match the table
    """
    for apath in inventory:
        if table_name+'.' in apath:
            return apath
    else:
        return None
    
def update_with_uri(file: str):
    """
    """
    new_lines=set()
    file_paths=build_all_file_inventory()
    with open(file, 'r') as f:
        old_entries=set(line.strip() for line in f)
        for base_name in old_entries:
            found=False
            for apath in file_paths:
                if base_name+'.' in apath:
                    new_lines.add(f"{base_name},{apath}")
                    found=True
                    break
            if not found:
                new_lines.add(f"{base_name},None")
    print(len(old_entries))
    return new_lines

if __name__ == "__main__":
    new_content=update_with_uri(TABLES_TO_PROCESS)
    with open(TABLES_TO_PROCESS, 'w') as file:
        for line in new_content:
            file.write(line + "\n")