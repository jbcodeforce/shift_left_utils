"""
Change the dedup sql statements in sources folder to adapt 
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

parser.add_argument('-f', '--src_name', required=True, help="name of the src folfer")

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

def process_src_file(sql_src_list: list[str]):
    for src_file_name in sql_src_list:
        the_path= Path(src_file_name)
        if "dml." in src_file_name:
            table_name = the_path.stem.split('.')[1]
            print(src_file_name, table_name)
            from_topic_content, sql_content = extract_reusable_content(src_file_name)
            change_dml(table_name, from_topic_content,sql_content, "")

def get_src_list(src_dir: str):
    file_paths=list_sql_files(f"{src_dir}")
    return file_paths


if __name__ == "__main__":
    print("#"*20)
   
    args = parser.parse_args()
    print(f"Build the list of dml in src folder in {args.src_name}")
    list_sql = get_src_list(args.src_name)
    process_src_file(list_sql)