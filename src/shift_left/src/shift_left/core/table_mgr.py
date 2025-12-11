"""
Copyright 2024-2025 Confluent, Inc.
"""
import os, re, json
from pathlib import Path
from jinja2 import Environment, PackageLoader
from typing import Tuple

from shift_left.core.pipeline_mgr import ( 
    read_pipeline_definition_from_file, 
    PIPELINE_JSON_FILE_NAME,
    PIPELINE_FOLDER_NAME)
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.table_worker import TableWorker
from shift_left.core.utils.sql_parser import SQLparser
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult
from shift_left.core.utils.file_search import (
    FlinkTableReference, 
    get_or_build_source_file_inventory, 
    get_or_build_inventory,
    SCRIPTS_DIR, 
    get_table_ref_from_inventory,
    extract_product_name,
    get_table_type_from_file_path,
    create_folder_if_not_exist,
    from_pipeline_to_absolute,
    build_inventory)



"""
Table management is for managing table folder, and table content as well as the table inventory and action to
Flink Table drop table. 

Naming convention for table name:
- For source tables: src_<product_name>_<table_name>
- For intermediate tables: int_<product_name>_<table_name>
- For fact tables: <product_name>_fct_<table_name>
- For dimension tables: <product_name>_dim_<table_name>
- For view tables: <product_name>_mv_<table_name>

Naming convention for ddl and dml file names
- Sources: `<ddl|dml>.src_<product>_<table_name>.sql`
- Intermediates: `<ddl|dml>.int_<product>_<table_name>.sql`
- Facts, Dimensions & Views: `<ddl|dml>.<product>_<fct|dim|mv>_<table_name>.sql`
"""

# --------- Public APIs ---------------
def build_folder_structure_for_table(table_folder_name: str, 
                                    target_path: str,
                                    product_name: str) -> Tuple[str, str]:
    """
    Create the folder structure for the given table name, under the target path. The structure looks like:
    
    * `target_path/product_name/table_name/sql-scripts/`:  with two template files, one for ddl. and one for dml.
    * `target_path/product_name/table_name/tests`: for test harness content
    * `target_path/product_name/table_name/Makefile`: a makefile to do Flink SQL statement life cycle management
    """
    logger.info(f"Create folder {table_folder_name} in {target_path}")
    table_type = get_table_type_from_file_path(target_path)
    if not product_name:
        table_folder = f"{target_path}/{table_folder_name}"
        product_name= extract_product_name(table_folder) 
    else:
        table_folder = f"{target_path}/{product_name}/{table_folder_name}"

   
    create_folder_if_not_exist(f"{table_folder}/" +  SCRIPTS_DIR)
    create_folder_if_not_exist(f"{table_folder}/tests")
    internal_table_name=get_long_table_name(table_folder_name, product_name, table_type)

    prefix= _get_statement_prefix(product_name)
    _create_makefile(internal_table_name, table_folder, prefix)
    _create_tracking_doc(internal_table_name, "", table_folder)
    _create_ddl_skeleton(internal_table_name, table_folder)
    _create_dml_skeleton(internal_table_name, table_folder)
    _create_dml_properties_skeleton(internal_table_name, table_folder)
    logger.debug(f"Created folder {table_folder} for the table {table_folder_name}")
    return table_folder, internal_table_name

def search_source_dependencies_for_dbt_table(sql_file_name: str, src_project_folder: str) -> str:
    """
    Search from the source project the dependencies for a given table.
    """
    logger.info(f"Search source dependencies for table {sql_file_name} in {src_project_folder}")
    with open(sql_file_name, "r") as f:
        sql_content = f.read()
        parser = SQLparser()
        table_name = parser.extract_table_name_from_insert_into_statement(sql_content)
        table_names = parser.extract_table_references(sql_content)
        if table_names:
            if table_name and table_name in table_names:
                table_names.remove(table_name)  # do not want to have self in dependencies
            all_src_files = get_or_build_source_file_inventory(src_project_folder)
            dependencies = []
            for table in table_names:
                if not table in all_src_files:
                    logger.error(f"Table {table} not found in the source project")
                    continue
                dependencies.append({"table": table, "src_dbt": all_src_files[table]})
            return dependencies
    return []

def get_short_table_name(src_file_name: str) -> Tuple[str,str,str]:
    """
    Extract the name of the table given a src file name
    """
    the_path= Path(src_file_name)
    table_name = the_path.stem
    parts = table_name.split("_")
    if len(parts) >= 3:
        if table_name.startswith("src_") or table_name.startswith("int_"):
            table_type = table_name.split("_")[0]
            product_name = table_name.split("_")[1]
            table_name = "_".join(table_name.split("_")[2:])
        else:
            product_name = table_name.split("_")[0]
            table_type = table_name.split("_")[1]
            table_name = "_".join(table_name.split("_")[2:])   
    else:
        table_type = ""
        product_name = ""
    return table_type, product_name, table_name

def get_long_table_name(table_name: str, product_name: str, table_type: str) -> str:
    """
    Get the table name for a given table name, product name and table type
    """
    if table_type == "fact":
        table_type = "fct"
        if table_name.startswith("fct_"):
            table_name= table_name.replace("fct_","",1)
        if product_name:
            return f"{product_name}_{table_type}_{table_name}"  
        else:
            return f"{table_type}_{table_name}"
    elif table_type == "dimension":
        table_type = "dim"
        if table_name.startswith("dim_"):
            table_name= table_name.replace("dim_","",1)
        if product_name:
            return f"{product_name}_{table_type}_{table_name}"
        else:
            return f"{table_type}_{table_name}"
    elif table_type == "intermediate":
        table_type = "int"
        if table_name.startswith("int_"):
            table_name= table_name.replace("int_","",1)
        if product_name:
            return f"{table_type}_{product_name}_{table_name}"
        else:
            return f"{table_type}_{table_name}"
    elif table_type == "view":
        table_type = "mv"
        return f"{product_name}_{table_type}_{table_name}"
    elif table_type == "source":
        table_type = "src"  
        if table_name.startswith("src_"):
            table_name= table_name.replace("src_","",1)
        if product_name:
            return f"{table_type}_{product_name}_{table_name}"
        else:
            return f"{table_type}_{table_name}"
    return table_name

def update_makefile_in_folder(pipeline_folder: str, table_name: str):
    inventory = get_or_build_inventory(pipeline_folder, pipeline_folder, False)
    if table_name not in inventory:
        logger.error(f"Table {table_name} not found in the pipeline inventory {pipeline_folder}")
        return
    existing_path = inventory[table_name]["table_folder_name"]
    table_folder = pipeline_folder.replace(PIPELINE_FOLDER_NAME,"",1) + "/" +  existing_path
    product_name=existing_path.split("/")[-2]
    prefix= _get_statement_prefix(product_name)
    _create_makefile( table_name, 
                    table_folder, 
                    prefix)

def update_all_makefiles_in_folder(folder_path: str) -> int:
    """
    Update all the Makefiles for all the tables in the pipeline folder
    """
    count=0
    for root, dirs, files in os.walk(folder_path):
        if 'Makefile' in files or 'sql-scripts' in dirs:
            table_name=os.path.basename(root)
            product_name=os.path.dirname(root).split("/")[-1]
            table_type = get_table_type_from_file_path(root)
            table_name=get_long_table_name(table_name, product_name, table_type)
            prefix= _get_statement_prefix(product_name)
            _create_makefile(table_name, root, prefix)
            count+=1
    logger.info(f"Updated {count} Makefiles for tables in {folder_path}")
    return count


def search_users_of_table(table_name: str, pipeline_folder: str) -> str:
    """
    When pipeline definitions is present for this table, return the list of children
    """
    inventory = get_or_build_inventory(pipeline_folder, pipeline_folder, False)
    tab_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, inventory)
    results =  read_pipeline_definition_from_file(tab_ref.table_folder_name+ "/" + PIPELINE_JSON_FILE_NAME).children
    output=f"## `{table_name}` is referenced in {len(results)} Flink SQL statements:\n"
    if len(results) == 0:
        output+="\n\t no table ... yet"
    else:
        for t in results:
            output+=f"\n* `{t.table_name}` with the DML: {t.dml_ref}\n"
    return output

def get_or_create_inventory(pipeline_folder: str):
    """
    Build the table inventory from the PIPELINES path. This is a service API for the CLI, so it is kept here even
    if it delegates to file_search.build_inventory
    """
    return build_inventory(pipeline_folder)

def validate_table_cross_products(rootdir: str):
    config=get_config()
    for product in config["app"]["products"]:
        sqls=  _get_sql_paths_files(rootdir,product)
        invalid_names= _validate_table_names(sqls)
        invalid_pipelines= _validate_pipelines(sqls, rootdir)
        print('-'*50 + product.upper() + '-'*50)
        if invalid_names:
            for sql_file, violations in invalid_names.items():
                for violation in violations:
                    print("{:65s} {:s}".format(sql_file, violation))
        if invalid_pipelines:
            for sql_file, violations in invalid_pipelines.items():
                for violation in violations:
                    print("{:65s} {:s}".format(sql_file, violation))


def update_sql_content_for_file(sql_file_name: str, 
                                processor: TableWorker, 
                                string_to_change_from: str= None, 
                                string_to_change_to: str= None) -> bool:
    """
    """
    sql_content= load_sql_content_from_file(sql_file_name)
    updated, new_content= processor.update_sql_content(sql_content, string_to_change_from, string_to_change_to)
    if updated:
        with open(sql_file_name, "w") as f:
            f.write(new_content)
    return updated


def load_sql_content_from_file(sql_file_name) -> str:
    with open(sql_file_name, "r") as f:
        return f.read()


def explain_table(table_name: str, compute_pool_id: str, persist_report: bool = True) -> dict:
    """
    Explain the table using the Flink CLI. Run explain select.
    """
    inventory_path = os.getenv("PIPELINES")
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    explain_report= _get_flink_execution_plan_explanation(table_ref, compute_pool_id, persist_report=persist_report)
    return explain_report

def explain_tables_for_product(product_name: str, compute_pool_id: str, persist_report: bool = False) -> dict:
    """
    Explain the tables in the directory using the Flink CLI
    """
    config = get_config()
    if not compute_pool_id:
        compute_pool_id = config["flink"]["compute_pool_id"]
    inventory_path = os.getenv("PIPELINES")
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    product_tables_report = {}
    for table_name, table_ref_dict in table_inventory.items():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref.product_name == product_name:
            print(f"Process {table_ref.table_name} of {table_ref.product_name}")
            explain_report = _get_flink_execution_plan_explanation(table_ref, compute_pool_id, persist_report=persist_report)
            product_tables_report[table_ref.table_name] = explain_report['trace']
            print(f"--> {_summarize_trace(explain_report['trace'])}")
    return product_tables_report

def explain_tables_for_list_of_tables(table_list_file_name: str, compute_pool_id: str, persist_report: bool = False) -> dict:
    """
    Explain the tables in the list of tables file using the Flink CLI
    """
    product_tables_report = {}
    inventory_path = os.getenv("PIPELINES")
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    with open(table_list_file_name, "r") as f:
        table_names = f.read().splitlines()
        for table_name in table_names:
            table_ref = FlinkTableReference(**table_inventory[table_name])
            print(f"Process {table_name}")
            explain_report = _get_flink_execution_plan_explanation(table_ref, compute_pool_id, persist_report=persist_report)
            product_tables_report[table_name] = explain_report['trace']
        print(f"--> {_summarize_trace(explain_report['trace'])}")
    return product_tables_report

# --------- Private APIs ---------------

def _get_flink_execution_plan_explanation(table_ref: FlinkTableReference, compute_pool_id: str, persist_report: bool = False) -> dict:
    config = get_config()
    if not compute_pool_id:
        compute_pool_id = config["flink"]["compute_pool_id"]
    dml_file = from_pipeline_to_absolute(table_ref.dml_ref)
    if not dml_file:
        logger.error(f"No DML file found for table {table_ref.table_name}")
        return
    explain_query = "EXPLAIN " + load_sql_content_from_file(dml_file)
    transformer = statement_mgr.get_or_build_sql_content_transformer()
    _, sql_out= transformer.update_sql_content(explain_query)
    statement_name = f"explain-{config['kafka']['cluster_type']}-{table_ref.table_name.replace('_', '-')}"
    statement_mgr.delete_statement_if_exists(statement_name)
    result = statement_mgr.post_flink_statement(sql_content=sql_out, 
                                          statement_name=statement_name,
                                          compute_pool_id=compute_pool_id)
    explain_report= {'table_name': table_ref.table_name, 'trace': ''}
    if result and isinstance(result, Statement):
        if result.status.phase == "FAILED":
            logger.error(f"Failed to explain table {table_ref.table_name} error: {result.status.detail}")
            explain_report['trace']=result.status.detail             
        else:
            statement_results = statement_mgr.get_statement_results(statement_name)
            if statement_results and isinstance(statement_results, StatementResult) and statement_results.results.data:
                explain_report['trace']=statement_results.results.data[0].row[0]
                if persist_report:
                    with open(f"{shift_left_dir}/{table_ref.table_name}_explain_report.json", "w") as f:
                        f.write(json.dumps(explain_report))
            else:
                logger.error(f"Failed to get statement results: {statement_results}")

    return explain_report

def _create_tracking_doc(table_name: str, src_file_name: str,  out_dir: str):
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    tracking_tmpl = env.get_template(f"tracking_tmpl.jinja")
    context = {
        'table_name': table_name,
        'src_file_name': src_file_name,
    }
    rendered_tracking_md = tracking_tmpl.render(context)
    with open(out_dir + '/tracking.md', 'w') as f:
        f.write(rendered_tracking_md)

def _create_ddl_skeleton(table_name: str, out_dir: str):
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    ddl_name = "ddl." + table_name
    ddl_tmpl = env.get_template(f"create_table_skeleton.jinja")
    context = {
        'table_name': table_name,
        'default_PK': 'default_key',
        'column_definitions': '-- put here column definitions'
    }
    rendered_ddl = ddl_tmpl.render(context)
    with open(out_dir + '/sql-scripts/' + ddl_name + '.sql', 'w') as f:
        f.write(rendered_ddl)

def _create_dml_skeleton(table_name: str, out_dir: str):
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    dml_tmpl = env.get_template(f"dml_src_tmpl.jinja")
    dml_name = "dml." + table_name
    context = {
        'table_name': table_name,
        'sql_part': '-- part to select stuff',
        'where_part': '-- where condition or remove it',
        'src_table': 'src_table'
    }
    rendered_dml = dml_tmpl.render(context)
    with open(out_dir + '/sql-scripts/' + dml_name + ".sql", 'w') as f:
        f.write(rendered_dml)

def _create_dml_properties_skeleton(table_name: str, out_dir: str):
    dml_name = "dml." + table_name
    with open(out_dir + '/sql-scripts/' + dml_name + ".properties", 'w') as f:
        f.write("#An optional file to set multiple DML session properties, one per line as shown below.\n#sql.tables.scan.idle-timeout=1s")
    
def _get_statement_prefix (product_name: str):

    """
    Create a prefix for the statement name that is used in the Makefile script 
    """

    cluster_type = get_config().get('kafka').get('cluster_type')
    cloud_region = get_config().get('confluent_cloud').get('region')
    abbv_region = cloud_region.replace('-','').replace('east','e').replace('west','w').replace('south','s').replace('north','n').replace('central','c')
    if not product_name or product_name == "None":
        prefix=cluster_type + '-' + abbv_region
    else:
        prefix=cluster_type + '-' + abbv_region + '-' + product_name
    return prefix

def _create_makefile(table_name: str, 
                     out_dir: str, 
                     prefix: str):
    """
    Create a makefile to help deploy Flink statements for the given table name
    When the dml folder is called dedup the ddl should include a table name that is `_raw` suffix.
    """
    logger.debug(f"Create makefile for {table_name} in {out_dir}")
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    makefile_template = env.get_template(f"makefile_ddl_dml_tmpl.jinja")

    context = {
        'table_name': table_name,
        'script_pre_kebab': prefix
    }
    rendered_makefile = makefile_template.render(context)
    # Write the rendered Makefile to a file
    with open(out_dir + '/Makefile', 'w') as f:
        f.write(rendered_makefile)




def _get_sql_paths_files(folder_path: str, product: str) -> dict[str, any]:
    """
    Buuild a dict of filename with matching path to access the keyed file
    """
    sql_files_paths = {}
    for root, dirs, files in os.walk(folder_path):
        if product in root and "tests" not in root:
            for file in files:
                if file.startswith(('ddl','dml')) and file.endswith('.sql'):
                    sql_files_paths[file]=root
    return sql_files_paths

def _validate_table_names(sqls: dict) -> dict[str, any]:
    invalids={}
    #file_pattern=r"(ddl|dml)\.[a-zA-Z0-9]+(_[a-zA-Z0-9]+)*\.sql$"
    for name, path in sqls.items():
        violations=[]
        #--- Check sql file naming standards
        #if not re.match( file_pattern, name):
        #    violations.append('WRONG FILE Name')

        #--- Check sql files are in right dir
        if not 'sql-scripts' in path:
            violations.append('WRONG Directory ')
        sql_file_name=path+'/'+name

        substring='pipelines'
        index = path.find(substring)
        if index == -1:
            break
        before, after = path[:index], path[index + len(substring):]
        result = after.split("/")
        type=result[1]
        product=result[2]

        ct_violation = True
        key_context = False
        value_context = False
        #--- Check CREATE TABLE statement in DDL files
        if name.startswith('ddl'):
            with open(sql_file_name, "r") as f:
                changelog_mode = 'append'
                cleanup_policy = 'delete'
                sql_ltz = ''
                for line in f:
                    #removes multiple spaces & backticks
                    normalized_line = ' '.join(line.split()).replace("`", "").replace("'", "").replace(",", "")
                    if 'CREATE TABLE IF NOT EXISTS' in normalized_line:
                        ct_violation=False
                        table_name=normalized_line.split()[5]
                    elif 'changelog.mode' in normalized_line:
                        changelog_mode=normalized_line.split("=")[1].strip()
                    elif 'kafka.cleanup-policy' in normalized_line:
                        cleanup_policy=normalized_line.split("=")[1].strip()
                    elif 'sql.local-time-zone' in normalized_line:
                        sql_ltz=normalized_line.split("=")[1].strip()
                    elif 'key.avro-registry.schema-context' in normalized_line:
                        key_context=True
                    elif 'value.avro-registry.schema-context' in normalized_line:
                        value_context=True
            #print ('table(' + table_name + ')changelog(' + changelog_mode + ')cleanup(' + cleanup_policy + ')')
            if ct_violation:
                violations.append('CREATE TABLE statement')
            if not key_context:
                violations.append('key.avro-registry.schema-context NOT FOUND')
            if not value_context:
                violations.append('value.avro-registry.schema-context NOT FOUND')    

            match type:
                case 'sources':
                    file_pattern=r"^(ddl|dml)\.src_" + re.escape(product) + r"(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        #print ('name :' + name + 'product :' + product)
                        violations.append('WRONG FILE NAME ')
                    if not table_name.startswith('src_'+product):
                        violations.append('WRONG TABLE NAME : ' + table_name)
                    if not changelog_mode.startswith('upsert'):
                        violations.append('WRONG changelog.mode : ' + changelog_mode)
                    if not cleanup_policy.startswith('delete'):
                        violations.append('WRONG kafka.cleanup-policy : ' + cleanup_policy)
                case 'intermediates':
                    file_pattern=r"^(ddl|dml)\.int_" + re.escape(product) + r"(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                    if not table_name.startswith('int_'+product) or table_name.endswith('_deduped'):
                        violations.append('WRONG TABLE NAME : ' + table_name)
                    if not changelog_mode.startswith('upsert'):
                        violations.append('WRONG changelog.mode : ' + changelog_mode)
                    if not cleanup_policy.startswith('delete'):
                        violations.append('WRONG kafka.cleanup-policy : ' + cleanup_policy)
                case 'facts':
                    file_pattern=r"^(ddl|dml)\." + re.escape(product) + r"_fct(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                    if not table_name.startswith(product+'_fct'):
                        violations.append('WRONG TABLE NAME : ' + table_name)
                    if not changelog_mode.startswith('retract'):
                        violations.append('WRONG changelog.mode : ' + changelog_mode)
                    if not cleanup_policy.startswith('compact'):
                        violations.append('WRONG kafka.cleanup-policy : ' + cleanup_policy)
                    #--enable this later
                    #if not sql_ltz.startswith('UTC'):
                    #    violations.append('SINK tables sql.local-time-zone should be UTC : ' + sql_ltz)
                case 'dimensions':
                    file_pattern=r"^(ddl|dml)\." + re.escape(product) + r"_dim(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                    if not table_name.startswith(product+'_dim'):
                        violations.append('WRONG TABLE NAME : ' + table_name)
                    if not changelog_mode.startswith('retract'):
                        violations.append('WRONG changelog.mode : ' + changelog_mode)
                    if not cleanup_policy.startswith('compact'):
                        violations.append('WRONG kafka.cleanup-policy : ' + cleanup_policy)
                    #--enable this later
                    #if not sql_ltz.startswith('UTC'):
                    #    violations.append('SINK tables sql.local-time-zone should be UTC : ' + sql_ltz)
                case 'views':
                    file_pattern=r"^(ddl|dml)\." + re.escape(product) + r"_mv(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                    if not table_name.startswith(product+'_mv'):
                        violations.append('WRONG TABLE NAME : ' + table_name)
                    if not changelog_mode.startswith('retract'):
                        violations.append('WRONG changelog.mode : ' + changelog_mode)
                    if not cleanup_policy.startswith('compact'):
                        violations.append('WRONG kafka.cleanup-policy : ' + cleanup_policy)
                    #--enable this later
                    #if not sql_ltz.startswith('UTC'):
                    #    violations.append('SINK tables sql.local-time-zone should be UTC : ' + sql_ltz)
        elif name.startswith('dml'):
            match type:
                case 'sources':
                    file_pattern=r"^(ddl|dml)\.src_" + re.escape(product) + r"(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                case 'intermediates':
                    file_pattern=r"^(ddl|dml)\.int_" + re.escape(product) + r"(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                case 'facts':
                    file_pattern=r"^(ddl|dml)\." + re.escape(product) + r"_fct(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                case 'dimensions':
                    file_pattern=r"^(ddl|dml)\." + re.escape(product) + r"_dim(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')
                case 'views':
                    file_pattern=r"^(ddl|dml)\." + re.escape(product) + r"_mv(_[a-zA-Z0-9]+)*\.sql$"
                    if not re.match( file_pattern, name):
                        violations.append('WRONG FILE NAME ')

        if violations:
            invalids[name]=violations

    return invalids

def _validate_pipelines(sqls: dict, rootdir: str) -> dict[str, any]:
    invalids={}
    pipeline_path=os.path.dirname(rootdir)
    for name, path in sqls.items():
        if name.startswith('dml'):
            violations=[]
            pipeline_file=os.path.dirname(path)+'/pipeline_definition.json'
            if not os.path.exists(pipeline_file):
                violations.append('MISSING pipeline definition')
            else:
                try:
                    with open(pipeline_file, 'r') as file:
                        pipeline_definition=json.load(file)
                    if pipeline_definition:
                        table_name=pipeline_definition["table_name"]
                        if table_name == "No-Table":
                            violations.append('INVALID pipeline table_name : ' + table_name)
                        ddl_ref=pipeline_path+'/'+pipeline_definition["ddl_ref"]
                        if not os.path.exists(ddl_ref):
                            violations.append('INVALID pipeline ddl_ref ')
                        dml_ref=pipeline_path+'/'+pipeline_definition["dml_ref"]
                        if not os.path.exists(dml_ref):
                            violations.append('INVALID pipeline dml_ref ')
                        parents=pipeline_definition["parents"]
                        childrens=pipeline_definition["children"]

                except json.JSONDecodeError:
                    violations.append('INVALID pipeline json')

            if violations:
                invalids[name]=violations

    return invalids

def get_column_definitions(table_name: str) -> tuple[str,str]:
    return "-- put here column definitions", "-- put here column definitions"

def _summarize_trace(trace: str) -> str:
    if not trace:
        return ""
    lines = trace.split('\n')
    summary = ""
    for idx, line in enumerate(lines):
        if "The primary key does not match the upsert key derived from the query" in line:
            return line
        if "SQL validation failed" in line:
            summary += line
            summary += lines[idx+1]
            summary += lines[idx+2]
            return summary
    if not "ERROR" in trace or not "Warning" in trace:
        return ""
    return trace
    
