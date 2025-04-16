"""
Copyright 2024-2025 Confluent, Inc.

Interface definition to support modifying SQL code to multiple sql statements.
"""
import logging
from typing import Tuple
import re
from shift_left.core.utils.app_config import logger, get_config

class TableWorker():
    """
    Worker to update the content of a sql content, applying some specific logic
    """
    def update_sql_content(sql_content: str) -> Tuple[bool, str]:
        return (False, sql_content)
    
     
class ChangeChangeModeToUpsert(TableWorker):              
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(self, sql_content: str)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(re.escape("with ("), re.IGNORECASE)
        if not 'changelog.mode' in sql_content:
            sql_out=with_statement.sub("WITH (\n   'changelog.mode' = 'upsert',", sql_content)
            updated = True
        else:
            for line in sql_content.split('\n'):
                if 'changelog.mode' in line:
                    sql_out+="   'changelog.mode' = 'upsert',"
                    updated = True
                else:
                    sql_out+=line + "\n"
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     

class ChangePK_FK_to_SID(TableWorker):
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(self, sql_content: str)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        if '_pk_fk' in sql_content:
            sql_out=sql_content.replace("_pk_fk", "_sid")
            updated = True
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     

class Change_Concat_to_Concat_WS(TableWorker):
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(sql_content: str)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(re.escape("md5(concat("), re.IGNORECASE)
        if 'md5(concat(' in sql_content:
            sql_out=with_statement.sub("MD5(CONCAT_WS(''',", sql_content)
            updated = True
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     
class Change_CompressionType(TableWorker):
     """
     Predefined class to change the DDL setting for a 'c
     """
     def update_sql_content(self, sql_content: str)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(re.escape("with ("), re.IGNORECASE)
        if not 'kafka.producer.compression.type' in sql_content:
            sql_out=with_statement.sub("WITH (\n        'kafka.producer.compression.type' = 'snappy',", sql_content)
            updated = True
        else:
            for line in sql_content.split('\n'):
                if "'kafka.producer.compression.type'" in line and not  'snappy' in line:
                    sql_out+="         'kafka.producer.compression.type' = 'snappy',\n"
                    updated = True
                else:
                    sql_out+=line + "\n"
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     
class Change_SchemaContext(TableWorker):
     """
     Predefined class to change the DDL setting for the schema-context
     """
     def update_sql_content(self, sql_content: str)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        with_statement = re.compile(re.escape("with ("), re.IGNORECASE)
        if not 'key.avro-registry.schema-context' in sql_content:
            sql_out=with_statement.sub("WITH (\n   'key.avro-registry.schema-context' = '.flink-dev',\n   'value.avro-registry.schema-context' = '.flink-dev',\n", sql_content)
            updated = True
        else:
            for line in sql_content.split('\n'):
                if 'key.avro-registry.schema-context' in line and not '.flink-dev' in line:
                    sql_out+="         'key.avro-registry.schema-context'='.flink-dev',"
                    updated = True
                if 'value.avro-registry.schema-context' in line and not '.flink-dev' in line:
                    sql_out+="         'value.avro-registry.schema-context'='.flink-dev',"
                    updated = True
                else:
                    sql_out+=line + "\n"
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     
class ReplaceEnvInSqlContent(TableWorker):
    env = "dev"

    """
    Special worker to update schema and src topic name in sql content
    """
    dml_replacements = {
        "stage": {
            "adapt": {
                "search": rf"(ap-.*?)-(dev)(\.)",
                "replace": rf"\1-{env}\3"
            }
        }
    }
    ddl_replacements = {
        "stage": {
            "schema-context": {
                "search": rf"(.flink)-(dev)",
                "replace": rf"\1-{env}"
            }
        }
    }

    def __init__(self):
        self.config = get_config()
        self.env = self.config.get('kafka',{'cluster_type': 'dev'}).get('cluster_type')
        # Update the replacements with the current env
        self.dml_replacements["stage"]["adapt"]["replace"] = rf"\1-{self.env}\3"
        self.ddl_replacements["stage"]["schema-context"]["replace"] = rf"\1-{self.env}"

    def update_sql_content(self, sql_content: str)  -> Tuple[bool, str]:
        logger.debug(f"{sql_content} in {self.env}")
        updated = False
        if "CREATE TABLE" in sql_content:
            if self.env in self.ddl_replacements:
                for k, v in self.ddl_replacements[self.env].items():
                    sql_content = re.sub(v["search"], v["replace"], sql_content)
                    updated = True
                    logger.debug(f"{k} , {v} ")
        else:
            for k, v in self.dml_replacements[self.env].items():
                sql_content = re.sub(v["search"], v["replace"], sql_content)
                updated = True
                logger.debug(f"{k} , {v} ")
        logger.debug(sql_content)
        return updated, sql_content