import re
from typing import Set
"""
Dedicated class to parse a SQL statement and extract elements like table name
"""

class SQLparser:
    def __init__(self):
        self.table_pattern = r'\b(\s*FROM|JOIN)\s+(\s*([a-zA-Z_][a-zA-Z0-9_]*\.)?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        self.cte_pattern_1 = r'WITH\s+(\w+)\s+AS\s*\('
        self.cte_pattern_2 = r'\s+(\w+)\s+AS\s*\('
        self.not_wanted_words= r'\b(\s*CROSS JOIN UNNEST)\s+(\s*([a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)'
        
    

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
        Remove junk words from the table name
        """
        for not_wanted_word in ['UNNEST']:
            if not_wanted_word in table_name.upper():
                return None
        return table_name.strip()

    def extract_table_references(self, sql_content) -> Set[str]:
        """
        Extract the table reference from the sql_content, using different reg expressions to
        do not consider CTE name and kafka topic name. To extract kafka topic name, it remove name with mulitple '.' in it.
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
            not_wanted=re.findall(self.not_wanted_words, sql_content, re.IGNORECASE)
            matches=set()
            for table in tables:
                retrieved_table=table[1].replace('`','')
                if retrieved_table.count('.') > 0:
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
            tb=tbname[0][1].replace("`","")
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
        
