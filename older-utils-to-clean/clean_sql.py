
import argparse, os
import fileinput

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Clean the ddl created'
)
parser.add_argument('-o', '--pipeline_folder_path', required=True, help="name of the folder output of the pipelines")



def list_sql_files(folder_path: str) -> list[str]:
    sql_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))   
    return sql_files

def process_ddl_file(file_path: str, sql_file: str):
    print(f"Process {sql_file}")
    with fileinput.input(sql_file, inplace=True) as f:
        for line in f:
            if "```" in line or "final AS (" in line or "SELECT * FROM final" in line:
                pass
            else:
                print(line,end='')

def rename_file(sql_file: str):
    if sql_file.endswith(".bak"):             
        os.rename(sql_file, sql_file[:-4])
    #if sql_file.endswith("."):             
    #    os.rename(sql_file, sql_file[:-1])

def delete_bak_files(folder_path: str):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".bak"):           
                os.remove(os.path.join(root, file))

if __name__ == "__main__":
    args = parser.parse_args()
    # From the folder path get the list of sql files
    sql_files = list_sql_files(args.pipeline_folder_path)
    for sql_file in sql_files:
        #rename_file(sql_file)
        #delete_bak_files(args.pipeline_folder_path)
        process_ddl_file(args.pipeline_folder_path, sql_file)