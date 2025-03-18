"""
Interface definition to support modifying SQL code to multiple sql statements.
"""
import logging

class TableWorker():
    """
    Worker to update the content of a sql content, applying some specific logic
    """
    def update_sql_content(sql_content: str) -> str:
        return sql_content
    
class ChangeLocalTimeZone(TableWorker):
     """
     Predefined class to change the DDL setting to get UTC as a time zone
     """
     def update_sql_content(sql_content: str) -> str:
        sql_out=sql_content.replace("WITH (", "WITH (\n\t'sql.local-time-zone' = 'UTC-0',")
        logging.debug(f"SQL transformed to {sql_out}")
        return sql_out
     
class ChangeChangeModeToUpsert(TableWorker):
     """
     Predefined class to change the DDL setting for a change log
     """
     def update_sql_content(sql_content: str) -> str:
        sql_out: str = ""
        if not 'changelog.mode' in sql_content:
            sql_out=sql_content.replace("WITH (", "WITH (\n   'changelog.mode' = 'upsert',")
        else:

            for line in sql_content.split('\n'):
                print(line)
                if 'changelog.mode' in line:
                    sql_out+="   'changelog.mode' = 'upsert',"
                else:
                    sql_out+=line
        logging.debug(f"SQL transformed to {sql_out}")
        return sql_out