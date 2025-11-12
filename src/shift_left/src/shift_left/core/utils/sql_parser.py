"""
Copyright 2024-2025 Confluent, Inc.
"""
import re
from typing import Set, List, Tuple, Dict, Any
from shift_left.core.models.flink_statement_model import FlinkStatementComplexity
from shift_left.core.utils.app_config import logger
"""
Dedicated class to parse a SQL statement and extract elements like table name
"""

class SQLparser:
    def __init__(self):
        # extract table name declared after FROM, JOIN, INNER JOIN, LEFT JOIN, CREATE TABLE IF NOT EXISTS, INSERT INTO
        # Updated to support table names with hyphens (e.g., clone.dev.ap-record-execution-dev.state.record)
        self.table_pattern = r'\b(\s*FROM|JOIN|LEFT JOIN|INNER JOIN|CREATE TABLE IF NOT EXISTS|INSERT INTO)\s+(\s*`?([a-zA-Z_][a-zA-Z0-9_-]*\.)*[a-zA-Z_][a-zA-Z0-9_-]*`?)'
        self.cte_pattern_1 = r'(?i)\bWITH\s+(\w+)\s+AS\s*\('
        self.cte_pattern_2 = r'(?i),\s*(\w+)\s+AS\s*\('
        # Pattern to identify the start of WITH clause for CTE removal
        self.cte_start_pattern = r'(?i)\bWITH\s+'
        self.not_wanted_words = r'\b(CROSS\s+JOIN\s+UNNEST)\s*\('
        # Pattern to identify specific SQL functions that use FROM as a keyword (TRIM, SUBSTRING, etc.)
        # Only target specific functions that legitimately use FROM as part of their syntax
        self.function_from_pattern = r'\b(TRIM|OVERLAY|SUBSTRING|EXTRACT)\s*\([^)]*FROM[^)]*\)'
        # Pattern to identify comparison operators that use FROM (IS [NOT] DISTINCT FROM)
        self.comparison_from_pattern = r'\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\s+\w+(?:\.\w+)?'


    def extract_table_references(self, sql_content: str, keep_topic_name: bool = False) -> Set[str]:
        """
        Extract the tables referenced from the sql_content, using different reg expressions to
        do not consider CTE name and kafka topic name. To extract kafka topic name, it removes
        name with multiple '.' in it.
        """
        sql_content=self._normalize_sql(sql_content)
        # look at dbt ref
        regex=r'ref\([\'"]([^\'"]+)[\'"]\)'
        matches = re.findall(regex, sql_content, re.IGNORECASE)
        if len(matches) == 0:
            # Remove SQL functions that contain FROM keyword (like TRIM(BOTH '[]' FROM value))
            # and comparison operators (like IS NOT DISTINCT FROM) to avoid false positive table name matches
            sql_content_filtered = re.sub(self.function_from_pattern, '', sql_content, flags=re.IGNORECASE)
            sql_content_filtered = re.sub(self.comparison_from_pattern, '', sql_content_filtered, flags=re.IGNORECASE)
            # Remove CROSS JOIN UNNEST patterns to avoid extracting UNNEST as a table name
            sql_content_filtered = re.sub(self.not_wanted_words, '', sql_content_filtered, flags=re.IGNORECASE)

            # look a Flink SQL references table name after from or join
            tables = re.findall(self.table_pattern, sql_content_filtered, re.IGNORECASE)
            ctes = self._extract_cte_names(sql_content_filtered)
            matches_set=set[str]()
            for table in tables:
                logger.debug(table)
                if 'REPLACE' in table[1].upper():
                    continue
                retrieved_table=table[1].replace('`','')
                if not keep_topic_name and retrieved_table.count('.') > 1:  # this may not be the best way to remove topic
                    continue
                if not retrieved_table in ctes:
                    matches_set.add(retrieved_table)
            return matches_set
        return set(matches)

    def extract_table_name_from_insert_into_statement(self, sql_content) -> str:
        logger.debug(f"sql_content: {sql_content}")
        sql_content=self._normalize_sql(sql_content)
        regex=r'\b(\s*INSERT INTO)\s+(\s*(`?[a-zA-Z0-9_][a-zA-Z0-9_]*`?\.)?`?[a-zA-Z0-9_][a-zA-Z0-9_]*`?)'
        tbname = re.findall(regex, sql_content, re.IGNORECASE)
        logger.debug(f"table name: {tbname}")
        if len(tbname) > 0:
            if tbname[0][1] and '`' in tbname[0][1]:
                tb=tbname[0][1].replace("`","")
            else:
                tb=tbname[0][1]
            return tb
        return "No-Table"

    def extract_table_name_from_create_statement(self, sql_content) -> str:
        sql_content=self._normalize_sql(sql_content)
        # This regex supports backticks starting the table name and allows no space after EXISTS keyword
        regex = r'\b(CREATE TABLE IF NOT EXISTS|CREATE TABLE|CREATE OR REPLACE TABLE)\s*`?\s*(\w+(?:\.\w+)?|`[a-zA-Z0-9_]+`(?:\.`[a-zA-Z0-9_]+`)?)'
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
            >>> build_column_metadata_from_sql_content(sql)
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
        match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[`"]?[\w.-]+[`"]?(?:\.[`"]?[\w.-]+[`"]?)*)\s*\((.*?)\)', sql_content, re.IGNORECASE | re.DOTALL)
        if not match:
            return {}

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


    def extract_primary_key_from_sql_content(self, sql_content: str) -> List[str]:
        """
        Extract the primary key from the sql_content
        """
        match_multiple = re.search(r'PRIMARY KEY\((.*?)\)', sql_content, re.IGNORECASE)

        if match_multiple:
            column_names_str_multiple = match_multiple.group(1)
            result = []
            for name in column_names_str_multiple.split(','):
                result.append(name.strip())
        else:
            result=["No primary key found in the statement."]
        return result


    def extract_statement_complexity(self, sql_content: str, state_form: str) -> FlinkStatementComplexity:
        """
        Extract the complexity of the statement by counting different types of joins
        """
        complexity = FlinkStatementComplexity()
        complexity.state_form = state_form

        if not sql_content:
            complexity.number_of_regular_joins = 0
            complexity.number_of_left_joins = 0
            complexity.number_of_right_joins = 0
            complexity.number_of_inner_joins = 0
            complexity.number_of_outer_joins = 0
            complexity.complexity_type = "Simple"
            return complexity

        # Normalize SQL content to remove comments and extra whitespace
        normalized_sql = self._normalize_sql(sql_content)

        # Count different types of joins using regex patterns (order matters to avoid double counting)
        left_joins = len(re.findall(r'\bLEFT\s+(?:OUTER\s+)?JOIN\b', normalized_sql, re.IGNORECASE))
        right_joins = len(re.findall(r'\bRIGHT\s+(?:OUTER\s+)?JOIN\b', normalized_sql, re.IGNORECASE))
        inner_joins = len(re.findall(r'\bINNER\s+JOIN\b', normalized_sql, re.IGNORECASE))
        # Only count standalone FULL OUTER JOIN, not LEFT/RIGHT OUTER JOIN
        outer_joins = len(re.findall(r'\bFULL\s+OUTER\s+JOIN\b', normalized_sql, re.IGNORECASE))
        # Count CROSS JOINs separately (these should not contribute to complexity)
        cross_joins = len(re.findall(r'\bCROSS\s+JOIN\b', normalized_sql, re.IGNORECASE))

        # Count all JOINs first, then subtract specific types to get regular JOINs
        all_joins = len(re.findall(r'\bJOIN\b', normalized_sql, re.IGNORECASE))
        regular_joins = all_joins - left_joins - right_joins - inner_joins - outer_joins - cross_joins

        # Calculate total joins (excluding CROSS JOINs as they are typically stateless)
        total_joins = left_joins + right_joins + inner_joins + outer_joins + regular_joins

        complexity.number_of_regular_joins = regular_joins
        complexity.number_of_left_joins = left_joins
        complexity.number_of_right_joins = right_joins
        complexity.number_of_inner_joins = inner_joins
        complexity.number_of_outer_joins = outer_joins

        # Determine complexity type based on join count
        if total_joins <= 2:
            complexity.complexity_type = "Simple"
        elif total_joins <= 4:
            complexity.complexity_type = "Medium"
        else:
            complexity.complexity_type = "Complex"

        return complexity

    def parse_insert_sql_to_dict(self, sql_content: str) -> Dict[str, List[Any]]:
        """
        Parse INSERT SQL statement and extract column names and their corresponding values.

        Args:
            sql_content: SQL INSERT statement as string

        Returns:
            Dictionary mapping column names to lists of values

        Example:
            For SQL: INSERT INTO table (col1, col2) VALUES ('val1', 'val2'), ('val3', 'val4')
            Returns: {'col1': ['val1', 'val3'], 'col2': ['val2', 'val4']}
        """
        # Remove comments and normalize whitespace
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        sql_content = ' '.join(sql_content.split())

        # Extract column names using regex
        # Matches: INSERT INTO table_name (col1, col2, col3) or INSERT INTO `table_name` (`col1`, `col2`, `col3`)
        column_pattern = r'insert\s+into\s+[`"]?\w+[`"]?\s*\(\s*([^)]+)\s*\)'
        column_match = re.search(column_pattern, sql_content, re.IGNORECASE)

        if not column_match:
            raise ValueError("Could not extract column names from INSERT statement")

        # Clean column names (remove backticks, quotes, and whitespace)
        column_names_str = column_match.group(1)
        column_names = [
            col.strip().strip('`').strip('"').strip("'")
            for col in column_names_str.split(',')
        ]

        # Extract VALUES section
        values_pattern = r'values\s+(.+?)(?:;|\s*$)'
        values_match = re.search(values_pattern, sql_content, re.IGNORECASE | re.DOTALL)

        if not values_match:
            raise ValueError("Could not extract VALUES section from INSERT statement")

        values_section = values_match.group(1).strip()

        # Parse individual value rows
        # This regex matches parentheses containing values, handling nested quotes
        row_pattern = r'\(([^)]+)\)'
        rows = re.findall(row_pattern, values_section)

        if not rows:
            raise ValueError("Could not extract value rows from VALUES section")

        # Initialize result dictionary
        result = {col_name: [] for col_name in column_names}

        # Process each row
        for row in rows:
            # Split values by comma, but handle quoted strings properly
            values = self._parse_sql_values(row)

            if len(values) != len(column_names):
                raise ValueError(f"Number of values ({len(values)}) doesn't match number of columns ({len(column_names)})")

            # Add values to corresponding columns
            for i, value in enumerate(values):
                result[column_names[i]].append(value)

        return result

    # ------------------------------------------------------------
    # ---- private methods ----
    # ------------------------------------------------------------
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

    def _parse_sql_values(self, values_str: str) -> List[Any]:
        """
        Parse comma-separated SQL values, handling quotes and data types.

        Args:
            values_str: String containing comma-separated values

        Returns:
            List of parsed values (strings, numbers, booleans, None)
        """
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        i = 0

        while i < len(values_str):
            char = values_str[i]

            if char in ("'", '"') and not in_quotes:
                in_quotes = True
                quote_char = char
                i += 1
                continue
            elif char == quote_char and in_quotes:
                # Check for escaped quotes
                if i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                    current_value += char
                    i += 2
                    continue
                else:
                    in_quotes = False
                    quote_char = None
                    i += 1
                    continue
            elif char == ',' and not in_quotes:
                # End of current value
                values.append(current_value.strip())
                current_value = ""
                i += 1
                continue

            current_value += char
            i += 1

        # Add the last value
        if current_value.strip():
            values.append(current_value.strip())

        return values



    def _extract_cte_names(self, sql_script: str) -> List[str]:
        """
        Extract Common Table Expressions (CTEs) names from SQL script.
        Supports both uppercase and lowercase WITH and AS keywords.

        Args:
            sql_script (str): SQL script that may contain CTEs

        Returns:
            List[str]: List of CTE names

        Example:
            Input: "WITH cte1 AS (SELECT * FROM table1), cte2 AS (SELECT * FROM table2) SELECT * FROM cte1"
            Output: ["cte1", "cte2"]
        """
        if not sql_script or not sql_script.strip():
            return []

        # Normalize the SQL first
        sql = self._normalize_sql(sql_script)

        # Check if there's a WITH clause
        with_match = re.search(self.cte_start_pattern, sql)
        if not with_match:
            return []  # No CTEs found, return empty list

        cte_names = []

        # Extract the first CTE name using existing pattern
        first_cte_matches = re.findall(self.cte_pattern_1, sql)
        if first_cte_matches:
            cte_names.extend(first_cte_matches)

        # Extract subsequent CTE names using existing pattern
        subsequent_cte_matches = re.findall(self.cte_pattern_2, sql)
        if subsequent_cte_matches:
            cte_names.extend(subsequent_cte_matches)

        # Alternative approach: use a comprehensive pattern to catch all CTE names
        if not cte_names:
            # Fallback pattern to catch CTE names in various formats
            comprehensive_pattern = r'(?i)\b(?:WITH|,)\s+(\w+)\s+AS\s*\('
            all_matches = re.findall(comprehensive_pattern, sql)
            if all_matches:
                cte_names.extend(all_matches)

        # Remove duplicates while preserving order
        seen = set()
        unique_cte_names = []
        for name in cte_names:
            if name not in seen:
                seen.add(name)
                unique_cte_names.append(name)

        return unique_cte_names
