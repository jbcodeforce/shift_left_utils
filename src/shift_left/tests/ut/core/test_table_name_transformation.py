"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for table name transformation with postfix
"""
import unittest
import pathlib
import os
import re
from typing import Set
from unittest.mock import patch, MagicMock

# Set up test environment
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")

from shift_left.core.utils.sql_parser import SQLparser


class TestTableNameTransformation(unittest.TestCase):
    """Unit test suite for table name transformation with postfix preservation of backticks."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.POST_FIX_UNIT_TEST = "_ut"  # Mock the postfix value
        
    def extract_all_table_names(self, sql_content: str) -> Set[str]:
        """
        Extract table names from SQL using multiple approaches to handle DML and DDL statements.
        This mirrors the implementation in test_mgr.py
        """
        table_names = set()
        
        # Use the existing SQL parser for DML statements (SELECT, INSERT, etc.)
        parser = SQLparser()
        dml_tables = parser.extract_table_references(sql_content)
        table_names.update(dml_tables)
        
        # Additional patterns for comma-separated tables and schema.table formats in FROM clauses
        # This handles cases like "FROM table1, table2, table3" and "FROM schema.`table`"
        from_comma_pattern = r'\bFROM\s+((?:`?[a-zA-Z_][a-zA-Z0-9_]*(?:\.\s*`?[a-zA-Z_][a-zA-Z0-9_]*`?)*`?(?:\s*,\s*`?[a-zA-Z_][a-zA-Z0-9_]*(?:\.\s*`?[a-zA-Z_][a-zA-Z0-9_]*`?)*`?)*)+)'
        from_matches = re.findall(from_comma_pattern, sql_content, re.IGNORECASE)
        for match in from_matches:
            # Split by comma and extract individual table names
            table_list = re.split(r'\s*,\s*', match.strip())
            for table in table_list:
                # Extract all table/schema parts using a more comprehensive regex
                # This pattern matches both schema.table and individual table names, with optional backticks
                table_parts_pattern = r'`?([a-zA-Z_][a-zA-Z0-9_]*)`?'
                parts = re.findall(table_parts_pattern, table.strip())
                for part in parts:
                    if part and not part.isdigit():  # Avoid numeric values
                        table_names.add(part)
        
        # Additional patterns for DDL statements not handled by the SQL parser
        ddl_patterns = [
            r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(`?[a-zA-Z_][a-zA-Z0-9_]*`?)',  # DROP TABLE
            r'\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(`?[a-zA-Z_][a-zA-Z0-9_]*`?)',  # CREATE TABLE
            r'\bTRUNCATE\s+TABLE\s+(`?[a-zA-Z_][a-zA-Z0-9_]*`?)',  # TRUNCATE TABLE
            r'\bALTER\s+TABLE\s+(`?[a-zA-Z_][a-zA-Z0-9_]*`?)',  # ALTER TABLE
        ]
        
        for pattern in ddl_patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE)
            for match in matches:
                # Remove backticks for consistent handling
                clean_name = match.strip('`')
                table_names.add(clean_name)
        
        return table_names

    def replace_table_name_with_postfix(self, sql_content: str) -> str:
        """
        Test implementation of the table name replacement logic.
        This mirrors the actual implementation in test_mgr.py
        """
        table_names = self.extract_all_table_names(sql_content)
        
        # Sort table names by length (descending) to avoid substring replacement issues
        sorted_table_names = sorted(table_names, key=len, reverse=True)
        
        for table in sorted_table_names:
            # Use regex with capturing groups to preserve backticks
            # This pattern specifically handles backticks vs word boundaries separately
            escaped_table = re.escape(table)
            
            # Handle backticked table names
            backtick_pattern = r'`(' + escaped_table + r')`'
            sql_content = re.sub(backtick_pattern, f'`{table}{self.POST_FIX_UNIT_TEST}`', sql_content, flags=re.IGNORECASE)
            
            # Handle non-backticked table names with word boundaries
            word_pattern = r'\b(' + escaped_table + r')\b'
            
            def replacement_func(match):
                table_name = match.group(1)
                return f"{table_name}{self.POST_FIX_UNIT_TEST}"
            
            sql_content = re.sub(word_pattern, replacement_func, sql_content, flags=re.IGNORECASE)
        
        return sql_content

    def test_table_name_without_backticks(self):
        """Test table name transformation without backticks."""
        sql = "SELECT * FROM users WHERE id = 1"
        result = self.replace_table_name_with_postfix(sql)
        expected = "SELECT * FROM users_ut WHERE id = 1"
        self.assertEqual(result, expected)

    def test_table_name_with_backticks(self):
        """Test table name transformation with backticks preserved."""
        sql = "SELECT * FROM `users` WHERE id = 1"
        result = self.replace_table_name_with_postfix(sql)
        expected = "SELECT * FROM `users_ut` WHERE id = 1"
        self.assertEqual(result, expected)

    def test_multiple_table_names_mixed_backticks(self):
        """Test transformation with multiple tables, some with backticks."""
        sql = "SELECT u.name, o.amount FROM `users` u JOIN orders o ON u.id = o.user_id"
        result = self.replace_table_name_with_postfix(sql)
        expected = "SELECT u.name, o.amount FROM `users_ut` u JOIN orders_ut o ON u.id = o.user_id"
        self.assertEqual(result, expected)

    def test_table_name_in_create_statement(self):
        """Test table name transformation in CREATE TABLE statement."""
        sql = "CREATE TABLE `test_table` (id INT, name VARCHAR(50))"
        result = self.replace_table_name_with_postfix(sql)
        expected = "CREATE TABLE `test_table_ut` (id INT, name VARCHAR(50))"
        self.assertEqual(result, expected)

    def test_table_name_in_insert_statement(self):
        """Test table name transformation in INSERT statement."""
        sql = "INSERT INTO products (name, price) VALUES ('item1', 100)"
        result = self.replace_table_name_with_postfix(sql)
        expected = "INSERT INTO products_ut (name, price) VALUES ('item1', 100)"
        self.assertEqual(result, expected)

    def test_table_name_with_schema_prefix(self):
        """Test table name transformation with schema prefix."""
        sql = "SELECT * FROM schema1.`table_name` WHERE active = 1"
        result = self.replace_table_name_with_postfix(sql)
        # Note: The enhanced table extraction extracts both "schema1" and "table_name" as table references
        # so both get the postfix applied
        expected = "SELECT * FROM schema1_ut.`table_name_ut` WHERE active = 1"
        self.assertEqual(result, expected)

    def test_table_name_at_end_of_query(self):
        """Test table name transformation when table name is at the end."""
        sql = "DROP TABLE users"
        result = self.replace_table_name_with_postfix(sql)
        expected = "DROP TABLE users_ut"
        self.assertEqual(result, expected)

    def test_table_name_with_semicolon(self):
        """Test table name transformation followed by semicolon."""
        sql = "SELECT COUNT(*) FROM `analytics_data`;"
        result = self.replace_table_name_with_postfix(sql)
        expected = "SELECT COUNT(*) FROM `analytics_data_ut`;"
        self.assertEqual(result, expected)

    def test_complex_query_with_multiple_backticked_tables(self):
        """Test complex query with multiple backticked table names."""
        sql = """
        SELECT u.username, p.title, c.content 
        FROM `users` u
        JOIN `posts` p ON u.id = p.user_id
        LEFT JOIN `comments` c ON p.id = c.post_id
        WHERE u.active = 1
        """
        result = self.replace_table_name_with_postfix(sql)
        expected = """
        SELECT u.username, p.title, c.content 
        FROM `users_ut` u
        JOIN `posts_ut` p ON u.id = p.user_id
        LEFT JOIN `comments_ut` c ON p.id = c.post_id
        WHERE u.active = 1
        """
        self.assertEqual(result, expected)

    def test_table_name_partial_match_prevention(self):
        """Test that longer table names are replaced before shorter ones to prevent partial matches."""
        sql = "SELECT * FROM user_profiles, users WHERE user_profiles.user_id = users.id"
        result = self.replace_table_name_with_postfix(sql)
        expected = "SELECT * FROM user_profiles_ut, users_ut WHERE user_profiles_ut.user_id = users_ut.id"
        self.assertEqual(result, expected)

    def test_table_name_case_insensitive(self):
        """Test that table name replacement is case insensitive."""
        sql = "SELECT * FROM Users WHERE ID = 1"
        result = self.replace_table_name_with_postfix(sql)
        # Note: The replacement preserves the original case but finds matches case-insensitively
        expected = "SELECT * FROM Users_ut WHERE ID = 1"
        self.assertEqual(result, expected)

    def test_truncate_table_statement(self):
        """Test table name transformation in TRUNCATE TABLE statement."""
        sql = "TRUNCATE TABLE temp_data"
        result = self.replace_table_name_with_postfix(sql)
        expected = "TRUNCATE TABLE temp_data_ut"
        self.assertEqual(result, expected)

    def test_alter_table_statement(self):
        """Test table name transformation in ALTER TABLE statement."""
        sql = "ALTER TABLE `user_profiles` ADD COLUMN email VARCHAR(255)"
        result = self.replace_table_name_with_postfix(sql)
        expected = "ALTER TABLE `user_profiles_ut` ADD COLUMN email VARCHAR(255)"
        self.assertEqual(result, expected)

    def test_mixed_ddl_dml_statements(self):
        """Test table name transformation in mixed DDL and DML statements."""
        sql = """
        CREATE TABLE temp_users (id INT);
        INSERT INTO temp_users SELECT id FROM `users`;
        DROP TABLE temp_users;
        """
        result = self.replace_table_name_with_postfix(sql)
        expected = """
        CREATE TABLE temp_users_ut (id INT);
        INSERT INTO temp_users_ut SELECT id FROM `users_ut`;
        DROP TABLE temp_users_ut;
        """
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()