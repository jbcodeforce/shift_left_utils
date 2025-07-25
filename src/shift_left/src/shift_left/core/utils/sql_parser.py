"""
Copyright 2024-2025 Confluent, Inc.
"""
import re
from typing import Set, List, Tuple, Dict
from shift_left.core.utils.app_config import logger
"""
Dedicated class to parse a SQL statement and extract elements like table name
"""

class SQLparser:
    def __init__(self):
        self.table_pattern = r'\b(\s*FROM|JOIN|LEFT JOIN|CREATE TABLE IF NOT EXISTS|INSERT INTO)\s+(\s*([a-zA-Z_][a-zA-Z0-9_]*\.)?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        self.cte_pattern_1 = r'WITH\s+(\w+)\s+AS\s*\('
        self.cte_pattern_2 = r'\s+(\w+)\s+AS+\s*\('
        self.not_wanted_words = r'\b(CROSS\s+JOIN\s+UNNEST)\s*\('
    

    def _normalize_sql(self, sql_script):
        """
        Normalize SQL script by removing comments and extra whitespace
        Args:
            sql_script (str): Original SQL script
        Returns:
            str: Normalized SQL script
        """
        # Remove multiple line comments /* */
        sql = re.sub(r'/\*[^*]*\*+(?:[^*/][^*]*\*+)*/', ' ', sql_script)
        
        # Remove single line comments --
        sql = re.sub(r'--[^\n]*', ' ', sql)
        
        # Replace newlines with spaces
        sql = re.sub(r'\s+', ' ', sql)
        
        return sql.strip()

    def remove_junk_words(self, table_name: str) -> str:
        """
        Remove words not wanted as table name
        """
        for not_wanted_word in [' UNNEST ']:
            if not_wanted_word in table_name.upper():
                return None
        return table_name.strip()

    def extract_table_references(self, sql_content) -> Set[str]:
        """
        Extract the table reference from the sql_content, using different reg expressions to
        do not consider CTE name and kafka topic name. To extract kafka topic name, it removes 
        name with mulitple '.' in it.
        """
        sql_content=self._normalize_sql(sql_content)
        #regex = r'{{\s*ref\([\'"]([^\']+)[\'"]\)\s*}}'
        #regex= r'{{\s*ref\(["\']([^"\']+)"\')\s*}}'
        # look at dbt ref
        regex=r'ref\([\'"]([^\'"]+)[\'"]\)'
        matches = re.findall(regex, sql_content, re.IGNORECASE)
        if len(matches) == 0:
            # look a Flink SQL references table name after from or join
            tables = re.findall(self.table_pattern, sql_content, re.IGNORECASE)
            ctes1 = re.findall(self.cte_pattern_1, sql_content, re.IGNORECASE)
            ctes2 = re.findall(self.cte_pattern_2, sql_content, re.IGNORECASE)
            not_wanted="[UNNEST]"
            matches=set()
            for table in tables:
                logger.debug(table)
                if 'REPLACE' in table[1].upper():
                    continue
                retrieved_table=table[1].replace('`','')
                if retrieved_table.count('.') > 1:  # this may not be the best way to remove topic
                    continue
                if not retrieved_table in ctes1 and not retrieved_table in ctes2 and not retrieved_table in not_wanted:
                    table_name=self.remove_junk_words(retrieved_table)
                    if table_name is not None:
                        matches.add(table_name)
            return matches
        return matches

    def extract_table_name_from_insert_into_statement(self, sql_content) -> str:
        sql_content=self._normalize_sql(sql_content)
        regex=r'\b(\s*INSERT INTO)\s+(\s*([`a-zA-Z_][a-zA-Z0-9_]*\.)?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        tbname = re.findall(regex, sql_content, re.IGNORECASE)
        if len(tbname) > 0:
            #logger.debug(tbname[0][1])
            if tbname[0][1] and '`' in tbname[0][1]:   
                tb=tbname[0][1].replace("`","")
            else:
                tb=tbname[0][1]
            return tb
        return "No-Table"
    
    def extract_table_name_from_create_statement(self, sql_content) -> str:
        sql_content=self._normalize_sql(sql_content)
        regex=r'\b(\s*CREATE TABLE IF NOT EXISTS)\s+(\s*([`a-zA-Z_][a-zA-Z0-9_]*\.)?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        tbname = re.findall(regex, sql_content, re.IGNORECASE)
        if len(tbname) > 0:
            #logger.debug(tbname[0][1])
            if tbname[0][1] and '`' in tbname[0][1]:   
                tb=tbname[0][1].replace("`","")
            else:
                tb=tbname[0][1]
            return tb
        return "No-Table"

    def parse_file(self, file_path):
        """
        Parse SQL file and extract table names
        Args:
            file_path (str): Path to SQL file
        Returns:
            list: List of unique table names found
        """
        try:
            with open(file_path, 'r') as file:
                sql_script = file.read()
            return self.extract_table_references(sql_script)
        except Exception as e:
            raise Exception(f"Error reading SQL file: {str(e)}")
        
    def extract_upgrade_mode(self, dml_sql_content, ddl_sql_content) -> str:
        """
        Extract the upgrade mode from the dml sql_content by analyzing it line by line.
        - CROSS JOIN UNNEST and CROSS JOIN LATERAL are considered stateless
        - Other JOINs and stateful operations make the query stateful
        For DDL content assert the change_log mode.
        """
        # Split into lines and normalize each line
        lines = dml_sql_content.split('\n')
        has_stateful_operation = False
        
        for line in lines:
            # Normalize the current line
            normalized_line = self._normalize_sql(line)
            
            # Skip empty lines
            if not normalized_line:
                continue
                
            # Check for other stateful operations
            if re.search(r'\b(JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|GROUP BY|TUMBLE|OVER|MATCH_RECOGNIZE)\s+', normalized_line, re.IGNORECASE):
                if not re.search(r'\bCROSS\s+JOIN\s+(?:UNNEST|LATERAL)\b', normalized_line, re.IGNORECASE):
                    # Check for CROSS JOIN UNNEST or CROSS JOIN LATERAL
                    has_stateful_operation = True
        if not has_stateful_operation:
            # Check for DDL content
            if ddl_sql_content:
                lines = ddl_sql_content.split('\n')
                for line in lines:
                    normalized_line = self._normalize_sql(line)
                    if 'changelog.mode' in normalized_line.lower() and ('upsert' in normalized_line.lower() or 'retract' in normalized_line.lower()):
                        return "Stateful"
                    
        return "Stateful" if has_stateful_operation else "Stateless"
    
    def build_column_metadata_from_sql_content(self, sql_content: str) -> Dict[str, Dict]:
        """
        Parse SQL CREATE TABLE statement and extract column definitions into a dictionary.
        
        Args:
            sql_content (str): The SQL CREATE TABLE statement content
            
        Returns:
            Dict[str, Dict]: Dictionary mapping column names to their definition details
                                     including type, nullability and primary key status
        Example:
            >>> sql = '''
            ... CREATE TABLE IF NOT EXISTS int_aqem_tag_tag_dummy_ut (
            ...     id                 STRING NOT NULL PRIMARY KEY,
            ...     tenant_id          STRING NOT NULL,
            ...     tag_key            STRING,
            ...     tag_value          STRING,
            ...     status             STRING
            ... )
            ... '''
            >>> parse_sql_columns(sql)
            [
                {'name': 'id', 'type': 'STRING', 'nullable': False, 'primary_key': True},
                {'name': 'tenant_id', 'type': 'STRING', 'nullable': False, 'primary_key': False},
                {'name': 'tag_key', 'type': 'STRING', 'nullable': True, 'primary_key': False},
                ...
            ]
        """
        # Remove comments and normalize whitespace, remove `
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        sql_content = re.sub(r'`', '', sql_content, flags=re.DOTALL)
        sql_content = re.sub(r'VARCHAR\(.*?\)', 'STRING', sql_content, flags=re.MULTILINE)
        sql_content = ' '.join(sql_content.split())
        
        # Extract the column definitions
        match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+)\s*\((.*?)\)', sql_content, re.IGNORECASE | re.DOTALL)
        if not match:
            return []

        columns_section = match.group(1)
        # Extract primary key columns from CONSTRAINT PRIMARY KEY or PRIMARY KEY clause
        prim_key_pattern = r'(?:CONSTRAINT\s+(?:PRIMARY\s+)?)?PRIMARY\s+KEY\s*\((.*?)\)'
        prim_key_match = re.search(prim_key_pattern, sql_content, re.IGNORECASE)
        prim_keys = prim_key_match.group(1) if prim_key_match else ''
        
        # Split into individual column definitions
        column_defs = [col.strip() for col in columns_section.split(',') if col.strip()]
        
        # Extract column details
        columns = {}
        for col_def in column_defs:
            # Skip constraints
            if col_def.upper().startswith(('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'CONSTRAINT')):
                continue
                
            parts = col_def.split()
            if len(parts) >= 2:
                col_name = parts[0].strip('`"[]')
                col_type = parts[1].upper()
                
                col_def_info = {
                    'name': col_name,
                    'type': col_type,
                    'nullable': 'NOT NULL' not in col_def.upper(),
                    'primary_key': False
                }
                
                # Check for inline PRIMARY KEY
                if 'PRIMARY KEY' in col_def.upper():
                    col_def_info['primary_key'] = True
                # Check if column is in table-level PRIMARY KEY
                elif prim_keys and col_name in prim_keys:
                    col_def_info['primary_key'] = True
                    
                columns[col_def_info['name']]=col_def_info
        
        return columns


