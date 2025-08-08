
"""
SQL Content Processing Extension for Environment-Specific Transformations.

This module provides specialized SQL content processing capabilities for the Shift Left Utils
toolkit, enabling environment-specific transformations of Flink SQL statements during
deployment across different environments (dev, stage, prod).

The main functionality includes:
- Schema context adaptations for different environments
- Topic prefix modifications based on deployment target
- Tenant filtering for development environments
- Thread-safe SQL transformations

Classes:
    ModifySqlContentForDifferentEnv: Main processor class for environment-specific SQL transformations

Example:
    >>> from shift_left.extensions.sql_content_processing import ModifySqlContentForDifferentEnv
    >>> processor = ModifySqlContentForDifferentEnv()
    >>> sql = "CREATE TABLE src_table (...) WITH ('flink-dev.schema')"
    >>> modified, transformed_sql = processor.update_sql_content(sql)
    >>> print(f"SQL was {'modified' if modified else 'unchanged'}")
"""

from typing import Tuple, Dict, Any, Optional
import re
from shift_left.core.utils.app_config import logger, get_config
import threading
from shift_left.core.utils.table_worker import TableWorker


class ModifySqlContentForDifferentEnv(TableWorker):
    """
    A specialized worker class for modifying SQL content based on deployment environment.
    
    This class extends TableWorker to provide environment-specific SQL transformations,
    including schema adaptations, topic name modifications, and environment-specific
    filtering logic for Flink SQL statements.
    
    Key Features:
    - Adapts schema references for different environments (dev, stage, prod)
    - Modifies topic prefixes and naming conventions based on environment
    - Adds tenant filtering for development environments
    - Thread-safe operations using semaphore
    
    Attributes:
        env (str): Current deployment environment (dev, stage, prod)
        topic_prefix (str): Kafka topic prefix for the environment
        product_name (str): Product identifier for filtering logic
        dml_replacements (dict): Environment-specific DML transformation rules
        ddl_replacements (dict): Environment-specific DDL transformation rules
        semaphore (threading.Semaphore): Thread synchronization primitive
        
    Example:
        >>> processor = ModifySqlContentForDifferentEnv()
        >>> updated, new_sql = processor.update_sql_content(sql_content, 'tenant_id', 'my_product')
        >>> if updated:
        ...     print(f"SQL was modified for {processor.env} environment")
    """
    
    # Default class attributes - these will be overridden during initialization
    env: str = "dev"  # Default environment
    topic_prefix: str = "clone"  # Default Kafka topic prefix
    product_name: str = "p1"  # Default product identifier
    
    # DML (Data Manipulation Language) transformation rules by environment
    # These patterns modify INSERT, SELECT, and other data operations
    dml_replacements: Dict[str, Dict[str, Dict[str, str]]] = {
        "stage": {
            "adapt": {
                # Transform topic references from dev to staging environment
                # Replaces patterns like "ap-east-1-dev." with "clone.stage.ap-east-1-stage."
                # Example: ap-east-1-dev -> clone.stage.ap-east-1-stage
                "search": r"^(.*?)(ap-.*?)-(dev)\.",
                # \1 captures prefix, \2 captures region pattern, \3 captures "dev"
                # Replacement includes topic_prefix and environment dynamically
                "replace": rf"\1{topic_prefix}.{env}.\2-{env}."
            }
        },
        "dev": {
            "adapt": {
                # Add tenant filtering for development environment
                # Transforms basic SELECT statements to include tenant-specific filtering
                "search": r"\s*select\s+\*\s+from\s+final\s*;?",
                "replace": rf"SELECT * FROM final WHERE tenant_id IN ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = '{product_name}')"
            }
        },
        "prod": {
            "adapt": {
                # Transform topic references from dev to production environment
                # Similar to staging but for production deployment
                "search": r"^(.*?)(ap-.*?)-(dev)\.",
                "replace": rf"\1{topic_prefix}.{env}.\2-{env}."
            }
        }
    }
    # DDL (Data Definition Language) transformation rules by environment
    # These patterns modify CREATE TABLE, schema references, and database contexts
    ddl_replacements: Dict[str, Dict[str, Dict[str, str]]] = {
        "stage": {
            "schema-context": {
                # Transform schema context references from dev to staging
                # Matches ".flink-dev" patterns in schema registry references
                "search": rf"(.flink)-(dev)",
                # Replace with environment-specific schema context
                # Example: .flink-dev -> .flink-stage in staging environment
                "replace": rf"\1-{env}"
            }
        },
        "prod": {
            "schema-context": {
                # Transform schema context references from dev to production
                # Same pattern as staging but for production deployment
                "search": rf"(.flink)-(dev)",
                "replace": rf"\1-{env}"
            }
        }
    }

    def __init__(self):
        """
        Initialize the SQL content processor with environment-specific configurations.
        
        Loads configuration from the application config and sets up environment-specific
        replacement patterns for SQL transformations. The initialization process:
        
        1. Retrieves the current environment and topic prefix from config
        2. Updates DML and DDL replacement patterns with current environment values
        3. Sets up thread synchronization with a semaphore
        4. Configures regex patterns for INSERT statement detection
        
        The replacement patterns are dynamically updated to include the actual
        environment and topic prefix values from the configuration.
        
        Raises:
            ConfigurationError: If required configuration values are missing
            
        Note:
            This constructor assumes the application configuration is properly
            initialized and accessible via get_config().
        """
        self.config: Dict[str, Any] = get_config()
        self.env: str = self.config.get('kafka', {'cluster_type': 'dev'}).get('cluster_type')
        self.topic_prefix: str = self.config.get('kafka', {'src_topic_prefix': 'clone'}).get('src_topic_prefix')
        
        # Update the replacements with the current env
        self.dml_replacements["stage"]["adapt"]["replace"] = rf"\1{self.topic_prefix}.{self.env}.\2-{self.env}."
        self.dml_replacements["prod"]["adapt"]["replace"] = rf"\1{self.topic_prefix}.{self.env}.\2-{self.env}."
        self.ddl_replacements["stage"]["schema-context"]["replace"] = rf"\1-{self.env}"
        self.ddl_replacements["prod"]["schema-context"]["replace"] = rf"\1-{self.env}"
        
        # Regex pattern for detecting INSERT INTO src_ statements
        self.insert_into_src = r"\s*INSERT\s+INTO\s+src_"
        
        # Thread-safe execution using semaphore (allows only one concurrent operation)
        self.semaphore = threading.Semaphore(value=1)


    def update_sql_content(self, sql_content: str, column_to_search: Optional[str] = None, product_name: Optional[str] = None) -> Tuple[bool, str]:
        """
        Transform SQL content based on the current deployment environment.
        
        This method applies environment-specific transformations to SQL statements,
        including schema adaptations, topic prefix modifications, and tenant filtering.
        The transformation logic differs between DDL (CREATE TABLE) and DML statements.
        
        Transformation Logic:
        1. DDL Statements (CREATE TABLE):
           - Updates schema context references (e.g., .flink-dev -> .flink-stage)
           
        2. DML Statements (INSERT, SELECT):
           - Removes 'clone.dev.' prefixes in non-dev environments
           - Adds tenant filtering for dev environment with specific products
           - Applies environment-specific topic naming patterns
        
        Args:
            sql_content (str): The original SQL statement to transform
            column_to_search (Optional[str], optional): Column name to search for in tenant filtering.
                Required for dev environment tenant filtering. Defaults to None.
            product_name (Optional[str], optional): Product identifier for environment-specific logic.
                Used in dev environment filtering and topic naming. Defaults to None.
                
        Returns:
            Tuple[bool, str]: A tuple containing:
                - bool: True if the SQL content was modified, False otherwise
                - str: The transformed SQL content
                
        Note:
            This method is thread-safe and uses a semaphore to ensure only one
            transformation occurs at a time. All transformations are logged
            for debugging purposes.
            
        Example:
            >>> processor = ModifySqlContentForDifferentEnv()
            >>> sql = "CREATE TABLE src_table (...) WITH ('flink-dev.schema')"
            >>> modified, new_sql = processor.update_sql_content(sql)
            >>> print(f"Modified: {modified}")
            >>> print(f"New SQL: {new_sql}")
        """
        with self.semaphore:
            logger.debug(f"{sql_content} in {self.env}")
            updated = False
            
            if "CREATE TABLE" in sql_content or "create table" in sql_content:
                # Handle DDL statements (CREATE TABLE)
                if self.env in self.ddl_replacements:
                    for k, v in self.ddl_replacements[self.env].items():
                        sql_content = re.sub(v["search"], v["replace"], sql_content)
                        updated = True
                        logger.debug(f"{k} , {v} ")
            else:
                # Handle DML statements (INSERT, SELECT, etc.)
                if 'clone.dev' in sql_content and self.env != 'dev':
                    # The sql content by default may use clone.dev. as the source topic prefix
                    # We need to remove the clone.dev. part when not on dev environment
                    sql_content = sql_content.replace('clone.dev.', '')
                    updated = True
                    
                if self.env == 'dev' and product_name not in ['stage', 'common']:
                    # Special handling for dev environment with tenant filtering
                    if re.search(self.insert_into_src, sql_content, re.IGNORECASE) and column_to_search in sql_content:
                        base_replace_str = str(self.dml_replacements["dev"]["adapt"]["replace"])
                        replace_str = base_replace_str.replace(self.product_name, product_name)
                        sql_out = re.sub(self.dml_replacements["dev"]["adapt"]["search"], replace_str, sql_content, flags=re.IGNORECASE)
                        updated = (sql_out != sql_content)
                        sql_content = sql_out
                        return updated, sql_content
                elif self.env in self.dml_replacements:
                    # Apply environment-specific DML transformations
                    for k, v in self.dml_replacements[self.env].items():
                        sql_out = re.sub(v["search"], v["replace"], sql_content, flags=re.MULTILINE)
                        updated = (sql_out != sql_content)
                        sql_content = sql_out
                        logger.info(f"{k} , {v} ")
                        
            logger.debug(sql_content)
            return updated, sql_content