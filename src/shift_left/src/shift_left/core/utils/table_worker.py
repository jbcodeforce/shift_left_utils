"""
Copyright 2024-2025 Confluent, Inc.

Interface definition to support modifying SQL code to multiple sql statements.
"""
from typing import Tuple
import re
from shift_left.core.utils.app_config import logger, get_config

class TableWorker():
    """
    Worker to update the content of a sql content, applying some specific logic
    """
    def update_sql_content(sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None) -> Tuple[bool, str]:
        return (False, sql_content)
    
     
class ChangeChangeModeToUpsert(TableWorker):              
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(self, sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(re.escape("with ("), re.IGNORECASE)
        if not 'changelog.mode' in sql_content:
            if not re.search(r"(?i)with\s*\(", sql_content):
                # Handle case where there's no WITH clause
                # Replace closing parenthesis with WITH clause
                sql_out = re.sub(r"\)\s*;?\s*$", ") WITH (\n   'changelog.mode' = 'upsert'\n);", sql_content)
                updated = True
            else:
                sql_out=with_statement.sub("WITH (\n       'changelog.mode' = 'upsert',\n", sql_content)
                updated = True
        else:
            for line in sql_content.split('\n'):
                if 'changelog.mode' in line:
                    sql_out+="   'changelog.mode' = 'upsert',\n"
                    updated = True
                else:
                    sql_out+=line + "\n"
        logger.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     

class ChangePK_FK_to_SID(TableWorker):
     """
     Predefined class to change _PK_FK suffix to _SID
     """
     def update_sql_content(self, sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        if '_pk_fk' in sql_content:
            sql_out=sql_content.replace("_pk_fk", "_sid")
            updated = True
        else:
            sql_out=sql_content
        logger.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     

class Change_Concat_to_Concat_WS(TableWorker):
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(r"md5\(concat\(", re.IGNORECASE)
        if 'md5(concat(' in sql_content.lower():
            sql_out=with_statement.sub("MD5(CONCAT_WS(''',", sql_content)
            updated = True
        logger.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     
class Change_CompressionType(TableWorker):
     """
     Predefined class to change the compression type
     """
     def update_sql_content(self, sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        
        with_statement = re.compile(r"(?i)with\s*\(|\)\s*with\s*\(")
        if not 'kafka.producer.compression.type' in sql_content:
            if not re.search(r"(?i)with\s*\(", sql_content):
                # Handle case where there's no WITH clause
                # Replace closing parenthesis with WITH clause
                sql_out = re.sub(r"\)\s*;?\s*$", ") WITH (\n        'kafka.producer.compression.type' = 'snappy');", sql_content)
                updated = True
            else:
                sql_out=with_statement.sub("WITH (\n        'kafka.producer.compression.type' = 'snappy',", sql_content)
                updated = True
        else:
            for line in sql_content.split('\n'):
                if "'kafka.producer.compression.type'" in line and not  'snappy' in line:
                    sql_out+="         'kafka.producer.compression.type' = 'snappy',\n"
                    updated = True
                else:
                    sql_out+=line + "\n"
        logger.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out

class DefaultStringReplacementInFromClause(TableWorker):
     """
     Predefined class to change the DML setting for one string to another in the from clause
     """
     def update_sql_content(self, sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        from_statement = re.compile(re.escape("from " + string_to_change_from), re.IGNORECASE)
        if from_statement.search(sql_content):
            sql_out=from_statement.sub("FROM " + string_to_change_to, sql_content)
            updated = True
        logger.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out

class Change_SchemaContext(TableWorker):
     """
     Predefined class to change the DDL setting for the schema-context
     """
     def update_sql_content(self, sql_content: str, string_to_change_from: str= None, string_to_change_to: str= None)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(re.escape("with ("), re.IGNORECASE)
        if not 'key.avro-registry.schema-context' in sql_content:
            if not re.search(r"(?i)with\s*\(", sql_content):
                # Handle case where there's no WITH clause
                # Replace closing parenthesis with WITH clause
                sql_out = re.sub(r"\)\s*;?\s*$", ") WITH (\n     'key.avro-registry.schema-context' = '.flink-dev',\n   'value.avro-registry.schema-context' = '.flink-dev'\n);", sql_content)
                updated = True
            else:
                sql_out=with_statement.sub("WITH (\n   'key.avro-registry.schema-context' = '.flink-dev',\n   'value.avro-registry.schema-context' = '.flink-dev',\n", sql_content)
                updated = True
        else:
            for line in sql_content.split('\n'):
                if 'key.avro-registry.schema-context' in line and not '.flink-dev' in line:
                    sql_out+="         'key.avro-registry.schema-context'='.flink-dev',\n"
                    updated = True
                elif 'value.avro-registry.schema-context' in line and not '.flink-dev' in line:
                    sql_out+="         'value.avro-registry.schema-context'='.flink-dev',\n"
                    updated = True
                else:
                    sql_out+=line + "\n"
        logger.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     
class ReplaceEnvInSqlContent(TableWorker):
    env = "dev"
    topic_prefix="clone"
    product_name="p1"
    """
    Special worker to update schema and src topic name in sql content depending of the environment.
    It also supports adding logic to filter out records for source topics depending on the environment.
    """
    dml_replacements = {
        "stage": {
            "adapt": {
                # Replaces (ap-.*?)-(dev) with clone.stage.(ap-.*?)
                # For example: ap-east-1-dev -> clone.stage.ap-east-1 in staging environment  
                "search": r"^(.*?)(ap-.*?)-(dev)\.",
                # \1 captures the first group (ap-.*?) from the search pattern
                # - adds a literal hyphen
                # {env} adds the environment value (e.g. "stage") {topic_prefix} adds the topic prefix (e.g. "clone")
                "replace": rf"\1{topic_prefix}.{env}.\2-{env}."
                #"search": r"^(.*){topic_prefix}\.dev\.(ap-.*?)-dev\.(.*)",
                #"replace": rf".\1{topic_prefix}.{env}.\2-{env}.\3"
            }
        },
        "dev": {
            "adapt": {
                "search": r"\s*select\s+\*\s+from\s+final\s*;?",
                "replace": rf"SELECT * FROM final WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = '{product_name}')"
            }
        },
        "prod": {
            "adapt": {
                "search": r"^(.*?)(ap-.*?)-(dev)\.",
                "replace": rf"\1{topic_prefix}.{env}.\2-{env}."
            }
        }
    }
    ddl_replacements = {
        "stage": {
            "schema-context": {
                "search": rf"(.flink)-(dev)",
                # Replaces .flink-dev with .flink-{env} in schema context
                # For example: .flink-dev -> .flink-stage in staging environment
                "replace": rf"\1-{env}"
            }
        },
        "prod": {
            "schema-context": {
                "search": rf"(.flink)-(dev)",
                "replace": rf"\1-{env}"
            }
        }
    }

    def __init__(self):
        self.config = get_config()
        self.env = self.config.get('kafka',{'cluster_type': 'dev'}).get('cluster_type')
        self.topic_prefix = self.config.get('kafka',{'src_topic_prefix': 'clone'}).get('src_topic_prefix')
        # Update the replacements with the current env
        self.dml_replacements["stage"]["adapt"]["replace"] = rf"\1{self.topic_prefix}.{self.env}.\2-{self.env}."
        self.dml_replacements["prod"]["adapt"]["replace"] = rf"\1{self.topic_prefix}.{self.env}.\2-{self.env}."
        self.ddl_replacements["stage"]["schema-context"]["replace"] = rf"\1-{self.env}"
        self.ddl_replacements["prod"]["schema-context"]["replace"] = rf"\1-{self.env}"
        self.insert_into_src=r"\s*INSERT\s+INTO\s+src_"


    def update_sql_content(self, sql_content: str, column_to_search: str= None, product_name: str= None)  -> Tuple[bool, str]:
        logger.debug(f"{sql_content} in {self.env}")
        updated = False
        if "CREATE TABLE" in sql_content or "create table" in sql_content:
            if self.env in self.ddl_replacements:
                for k, v in self.ddl_replacements[self.env].items():
                    sql_content = re.sub(v["search"], v["replace"], sql_content)
                    updated = True
                    logger.debug(f"{k} , {v} ")
        else:
            if 'clone.dev' in sql_content and self.env != 'dev':
                # the sql content by default may use clone.dev. as the source topic prefix
                # we need to remove the clone.dev. part when not on dev environment
                sql_content = sql_content.replace('clone.dev.', '')
                updated = True
            if self.env == 'dev':
                if re.search(self.insert_into_src, sql_content, re.IGNORECASE) and column_to_search in sql_content:
                    replace_str=self.dml_replacements["dev"]["adapt"]["replace"].replace(self.product_name,product_name)
                    self.dml_replacements["dev"]["adapt"]["replace"]=replace_str
                    sql_out = re.sub(self.dml_replacements["dev"]["adapt"]["search"], replace_str, sql_content, flags=re.IGNORECASE)
                    updated = (sql_out != sql_content)
                    sql_content=sql_out
                    return updated, sql_content
            elif self.env in self.dml_replacements:
                for k, v in self.dml_replacements[self.env].items():
                    sql_out = re.sub(v["search"], v["replace"], sql_content, flags=re.MULTILINE)
                    updated = (sql_out != sql_content)
                    sql_content=sql_out
                    logger.info(f"{k} , {v} ")
        logger.debug(sql_content)
        return updated, sql_content