"""
Change the source processing sql statements in sources folder to adapt to new table name, or change dedup logic
"""
import os, argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from find_path_for_table import list_sql_files
TMPL_NAME='templates/cte_src_tmpl.jinja'

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Change the content of source sql statement using the given template'
)

parser.add_argument('-s', '--src_name', required=False, help="name of the src folfer")
parser.add_argument('-w', '--where_condition', required=False, help="where condition using SQL syntax")
parser.add_argument('-on', '--old_name', required=False, help="The current table name")
parser.add_argument('-cn', '--change_name', required=False, help="The new table name")

def change_dml(table_name: str, src_topic_name: str, sql_part: str, where_part: str):
    context = {
        'table_name': table_name,
        'sql_part': sql_part,
        'src_topic': src_topic_name,
        'where_part': where_part
    }
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_NAME}")
    rendered_sql = sql_template.render(context)
    print(rendered_sql)

def extract_reusable_content(filename):
    from_topic_content=""
    sql_content=""
    with open(filename, "r") as f:
        for line in f.readlines():
            if 'INSERT INTO' in line:
                continue
            elif "FROM `clone" in line:
                from_topic_content = line
                continue
            else:
                sql_content+=line
    return from_topic_content, sql_content

def process_src_file(sql_src_list: list[str], table_name: str, where_condition: str):
    for src_file_name in sql_src_list:
        the_path= Path(src_file_name)
        if "dml." in src_file_name:
            table_name = the_path.stem.split('.')[1]
            print(src_file_name, table_name)
            from_topic_content, sql_content = extract_reusable_content(src_file_name)
            change_dml(table_name, from_topic_content, sql_content, where_condition)

def modify_ddl(src_file_name: str, new_table_name: str):
    with open(src_file_name, "r") as f:
        sql_content = f.read()
        old_table_name=sql_content.split(" ")[5]
        modified_sql_content = sql_content.replace(old_table_name, new_table_name)
        print(modified_sql_content)
    return old_table_name

def rename_table(sql_src_list: list[str], old_table_name: str, new_table_name: str):
    for src_file_name in sql_src_list:
        if "ddl." in src_file_name:
             with open(src_file_name, "r") as f:
                sql_content = f.read()
                modified_sql_content = sql_content.replace(old_table_name, new_table_name)
                print(modified_sql_content)        
        else:
            with open(src_file_name, "r") as f:
                sql_content = f.read()
                modified_sql_content = sql_content.replace(old_table_name, new_table_name)
                print(modified_sql_content)


def get_src_list(src_dir: str):
    """
    given the folder where the source tables are defined get the list of source path for each sql
    """
    file_paths=list_sql_files(f"{src_dir}")
    return file_paths



if __name__ == "__main__":
    print("#"*20)
   
    args = parser.parse_args()
    print(f"Build the list of dml in src folder in {args.src_name}")
    list_sql = get_src_list(args.src_name)
    if args.change_name:
        rename_table(list_sql, args.old_name, args.change_name)
    else:    
        process_src_file(list_sql, args.where_condition)