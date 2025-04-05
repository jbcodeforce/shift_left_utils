"""
Copyright 2024-2025 Confluent, Inc.

Interface definition to support modifying SQL code to multiple sql statements.
"""
import logging
from typing import Tuple

class TableWorker():
    """
    Worker to update the content of a sql content, applying some specific logic
    """
    def update_sql_content(sql_content: str) -> Tuple[bool, str]:
        return (False, sql_content)
    
class ChangeLocalTimeZone(TableWorker):
     """
     Predefined class to change the DDL setting to get UTC as a time zone
     """
     def update_sql_content(sql_content: str)  -> Tuple[bool, str]:
        updated = False
        if "WITH (" in sql_content:
            sql_out=sql_content.replace("WITH (", "WITH (\n\t'sql.local-time-zone' = 'UTC-0',")
            updated = True
            logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     
class ChangeChangeModeToUpsert(TableWorker):              
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(sql_content: str)  -> Tuple[bool, str]:
        updated = False
        sql_out: str = ""
        if not 'changelog.mode' in sql_content:
            sql_out=sql_content.replace("WITH (", "WITH (\n   'changelog.mode' = 'upsert',")
            updated = True
        else:
            for line in sql_content.split('\n'):
                if 'changelog.mode' in line:
                    sql_out+="   'changelog.mode' = 'upsert',"
                    updated = True
                else:
                    sql_out+=line
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out
     

class ChangePK_FK_to_SID(TableWorker):
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(sql_content: str)  -> Tuple[bool, str]:
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
        if 'md5(concat(' in sql_content:
            sql_out=sql_content.replace("md5(concat(", "MD5(CONCAT_WS(''',")
            updated = True
        logging.debug(f"SQL transformed to {sql_out}")
        return updated, sql_out