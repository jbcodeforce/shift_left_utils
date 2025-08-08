#!/usr/bin/env python3
"""
Spark SQL Script Validator

This script validates all the Spark SQL examples by:
1. Setting up a local Spark session
2. Creating sample data for all referenced tables
3. Executing each SQL script as a Spark job
4. Reporting success/failure for each script

Usage:
    python validate_spark_scripts.py
    
Requirements:
    pip install pyspark
"""

import os
import sys
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Tuple
from test_spark_scripts import TestSparkScripts
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, 
        StructField, 
        StringType, 
        TimestampType, 
        DateType, 
        DoubleType, 
        IntegerType
    )
except ImportError:
    print("ERROR: PySpark not installed. Please run: pip install pyspark")
    sys.exit(1)


class SparkSQLValidator:
    
    def __init__(self):
        """Initialize Spark session and setup"""
        self.spark = self._create_spark_session()
        self.results = []
        
    def _create_spark_session(self) -> SparkSession:
        """Create a local Spark session for testing"""
        return SparkSession.builder \
            .appName("SparkSQLValidator") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
    
    def _create_sample_data(self):
        """Create sample data for all tables referenced in the SQL scripts"""
        print("Creating sample data...")
        TestSparkScripts._create_web_events(self)
        TestSparkScripts._create_customer_profiles(self)
        TestSparkScripts._create_purchases(self)
        
        # Sample data for product analytics
        product_events_data = [
            ("prod_001", "electronics", datetime.strptime("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S"), "user_001", "page_view", 0.0),
            ("prod_001", "electronics", datetime.strptime("2024-01-15 10:35:00", "%Y-%m-%d %H:%M:%S"), "user_001", "purchase", 299.99),
            ("prod_002", "clothing", datetime.strptime("2024-01-15 11:00:00", "%Y-%m-%d %H:%M:%S"), "user_002", "page_view", 0.0),
            ("prod_002", "clothing", datetime.strptime("2024-01-15 11:05:00", "%Y-%m-%d %H:%M:%S"), "user_002", "add_to_cart", 0.0),
            ("prod_003", "books", datetime.strptime("2024-01-15 14:20:00", "%Y-%m-%d %H:%M:%S"), "user_003", "purchase", 24.99),
        ]
        
        product_events_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("revenue", DoubleType(), True)
        ])
        
        raw_product_events = self.spark.createDataFrame(product_events_data, product_events_schema)
        raw_product_events.createOrReplaceTempView("raw_product_events")
        
    
        
        
    
        
        # Sample data for event processing (with nested structures)
        raw_events_data = [
            ("evt_001", "user_001", "2024-01-15 10:00:00", "click", 
             {"tags": ["premium", "mobile"], "custom_fields": {"campaign_id": "camp_001", "source": "organic"}},
             {"device_info": {"os": "iOS", "browser": "Safari"}, "location": {"country": "US", "city": "New York"}},
             '{"user": {"preferences": {"language": "en"}}, "session": {"duration": "300"}}'),
            ("evt_002", "user_002", "2024-01-15 11:00:00", "purchase",
             {"tags": ["basic", "desktop"], "custom_fields": {"campaign_id": "camp_002", "source": "paid"}},
             {"device_info": {"os": "Windows", "browser": "Chrome"}, "location": {"country": "UK", "city": "London"}},
             '{"user": {"preferences": {"language": "en"}}, "session": {"duration": "450"}}'),
        ]
        
        # Create a simple version for raw_events (PySpark has limitations with complex nested structures in createDataFrame)
        raw_events_simple_data = [
            ("evt_001", "user_001", datetime.strptime("2024-01-15 10:00:00", "%Y-%m-%d %H:%M:%S"), "click"),
            ("evt_002", "user_002", datetime.strptime("2024-01-15 11:00:00", "%Y-%m-%d %H:%M:%S"), "purchase"),
            ("evt_003", "user_003", datetime.strptime("2024-01-15 12:00:00", "%Y-%m-%d %H:%M:%S"), "page_view"),
        ]
        
        raw_events_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        raw_events = self.spark.createDataFrame(raw_events_simple_data, raw_events_schema)
        raw_events.createOrReplaceTempView("raw_events")
        
        # Sales data
        sales_data = [
            ("electronics", "north", 1500.00, date(2024, 1, 15)),
            ("clothing", "south", 800.00, date(2024, 1, 15)),
            ("books", "east", 300.00, date(2024, 1, 15)),
            ("electronics", "west", 2200.00, date(2024, 1, 14)),
            ("clothing", "north", 950.00, date(2024, 1, 14)),
        ]
        
        sales_schema = StructType([
            StructField("product_category", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sale_amount", DoubleType(), True),
            StructField("sale_date", DateType(), True)
        ])
        
        sales_data_df = self.spark.createDataFrame(sales_data, sales_schema)
        sales_data_df.createOrReplaceTempView("sales_data")
        
        # Streaming events
        streaming_events_data = [
            (datetime.strptime("2024-01-15 10:00:00", "%Y-%m-%d %H:%M:%S"), "premium", "purchase", "web", "user_001", "session_001", 299.99, 200, 150.5, "US", "New York", "mobile"),
            (datetime.strptime("2024-01-15 10:05:00", "%Y-%m-%d %H:%M:%S"), "basic", "page_view", "mobile", "user_002", "session_002", 0.0, 404, 200.0, "UK", "London", "desktop"),
            (datetime.strptime("2024-01-15 10:10:00", "%Y-%m-%d %H:%M:%S"), "premium", "add_to_cart", "web", "user_003", "session_003", 0.0, 200, 180.2, "CA", "Toronto", "tablet"),
        ]
        
        streaming_events_schema = StructType([
            StructField("event_timestamp", TimestampType(), True),
            StructField("user_segment", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("revenue_amount", DoubleType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time_ms", DoubleType(), True),
            StructField("geo_country", StringType(), True),
            StructField("geo_city", StringType(), True),
            StructField("device_type", StringType(), True)
        ])
        
        streaming_events = self.spark.createDataFrame(streaming_events_data, streaming_events_schema)
        streaming_events.createOrReplaceTempView("streaming_events")
        
        # User activities
        user_activities_data = [
            ("user_001", "user001@email.com", date(2023, 1, 15), date(2024, 1, 15), "premium_feature"),
            ("user_002", "user002@email.com", date(2023, 6, 20), date(2024, 1, 10), "basic_feature"),
            ("user_003", "user003@email.com", date(2024, 1, 1), date(2024, 1, 14), "premium_feature"),
        ]
        
        user_activities_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("registration_date", DateType(), True),
            StructField("last_activity_date", DateType(), True),
            StructField("feature_used", StringType(), True)
        ])
        
        user_activities = self.spark.createDataFrame(user_activities_data, user_activities_schema)
        user_activities.createOrReplaceTempView("user_activities")
        
        # Feature usage
        feature_usage_data = [
            ("premium_feature", "user_001"),
            ("basic_feature", "user_002"),
            ("premium_feature", "user_003"),
        ]
        
        feature_usage_schema = StructType([
            StructField("feature_name", StringType(), True),
            StructField("user_id", StringType(), True)
        ])
        
        feature_usage = self.spark.createDataFrame(feature_usage_data, feature_usage_schema)
        feature_usage.createOrReplaceTempView("feature_usage")
        
        # User events for temporal analytics
        user_events_data = [
            ("user_001", datetime.strptime("2024-01-15 10:00:00", "%Y-%m-%d %H:%M:%S"), "purchase", 299.99),
            ("user_001", datetime.strptime("2024-01-14 15:30:00", "%Y-%m-%d %H:%M:%S"), "page_view", 0.0),
            ("user_002", datetime.strptime("2024-01-15 11:00:00", "%Y-%m-%d %H:%M:%S"), "purchase", 149.99),
            ("user_003", datetime.strptime("2024-01-13 09:15:00", "%Y-%m-%d %H:%M:%S"), "add_to_cart", 0.0),
        ]
        
        user_events_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("revenue_amount", DoubleType(), True)
        ])
        
        user_events = self.spark.createDataFrame(user_events_data, user_events_schema)
        user_events.createOrReplaceTempView("user_events")
        
        # Raw transactions for advanced transformations
        raw_transactions_data = [
            ("txn_001", "user_001", "prod_001", datetime.strptime("2024-01-15 10:00:00", "%Y-%m-%d %H:%M:%S"), 299.99, "USD", "electronics", "credit_card",
             '{"device": {"fingerprint": "fp001"}, "location": {"ip_address": "192.168.1.1"}, "risk_scores": {"fraud_score": "0.1"}}'),
            ("txn_002", "user_002", "prod_002", datetime.strptime("2024-01-15 11:00:00", "%Y-%m-%d %H:%M:%S"), 149.99, "EUR", "clothing", "paypal",
             '{"device": {"fingerprint": "fp002"}, "location": {"ip_address": "192.168.1.2"}, "risk_scores": {"fraud_score": "0.6"}}'),
            ("txn_003", "user_003", "prod_003", datetime.strptime("2024-01-15 12:00:00", "%Y-%m-%d %H:%M:%S"), 24.99, "GBP", "books", "debit_card",
             '{"device": {"fingerprint": "fp003"}, "location": {"ip_address": "192.168.1.3"}, "risk_scores": {"fraud_score": "0.9"}}'),
        ]
        
        raw_transactions_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("transaction_timestamp", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("transaction_metadata", StringType(), True)
        ])
        
        raw_transactions = self.spark.createDataFrame(raw_transactions_data, raw_transactions_schema)
        raw_transactions.createOrReplaceTempView("raw_transactions")
        
        print("✓ Sample data created successfully")
    
    def _read_sql_file(self, file_path: Path) -> str:
        """Read SQL content from file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            raise Exception(f"Failed to read file {file_path}: {e}")
    
    def _execute_sql_script(self, script_name: str, sql_content: str) -> Tuple[bool, str, int]:
        """Execute a SQL script and return success status, message, and row count"""
        try:
            # Clean up the SQL (remove comments, empty lines)
            sql_lines = [line.strip() for line in sql_content.split('\n') if line.strip() and not line.strip().startswith('--')]
            cleaned_sql = '\n'.join(sql_lines)
            
            if not cleaned_sql:
                return False, "Empty SQL content after cleaning", 0
            
            # Execute the SQL
            print(f"  Executing: {script_name}")
            result_df = self.spark.sql(cleaned_sql)
            
            # Try to collect some results to validate the query works
            row_count = result_df.count()
            
            # Show a few sample rows for debugging (optional)
            if row_count > 0:
                print(f"    ✓ Query executed successfully, returned {row_count} rows")
                # Uncomment the next line to see sample data:
                result_df.show(5, truncate=False)
            else:
                print(f"    ✓ Query executed successfully, returned 0 row")
            
            return True, f"Success: {row_count} rows returned", row_count
            
        except Exception as e:
            error_msg = f"Failed to execute SQL: {str(e)}"
            print(f"    ✗ {error_msg}")
            # Print more detailed error for debugging
            print(f"    Error details: {traceback.format_exc()}")
            return False, error_msg, 0
    
    def validate_script(self, script_path: Path) -> Dict:
        """Validate a single SQL script"""
        script_name = script_path.name
        print(f"\n🔍 Validating: {script_name}")
        
        try:
            sql_content = self._read_sql_file(script_path)
            success, message, row_count = self._execute_sql_script(script_name, sql_content)
            
            result = {
                'script': script_name,
                'path': str(script_path),
                'success': success,
                'message': message,
                'row_count': row_count,
                'timestamp': datetime.now().isoformat()
            }
            
            self.results.append(result)
            return result
            
        except Exception as e:
            error_msg = f"Validation failed: {str(e)}"
            print(f"  ✗ {error_msg}")
            
            result = {
                'script': script_name,
                'path': str(script_path),
                'success': False,
                'message': error_msg,
                'row_count': 0,
                'timestamp': datetime.now().isoformat()
            }
            
            self.results.append(result)
            return result
    
    def validate_all_scripts(self, sources_dir: Path) -> List[Dict]:
        """Validate all SQL scripts in the sources directory"""
        print("🚀 Starting Spark SQL Script Validation")
        print("=" * 50)
        
        # Create sample data first
        self._create_sample_data()
        
        # Find all SQL files
        sql_files = list(sources_dir.glob("src_*.sql"))
        
        if not sql_files:
            print(f"No SQL files found in {sources_dir}")
            return []
        
        print(f"\nFound {len(sql_files)} SQL scripts to validate:")
        for sql_file in sql_files:
            print(f"  - {sql_file.name}")
        
        # Validate each script
        for sql_file in sql_files:
            self.validate_script(sql_file)
        
        return self.results
    
    def print_summary(self):
        """Print validation summary"""
        print("\n" + "=" * 50)
        print("📊 VALIDATION SUMMARY")
        print("=" * 50)
        
        total_scripts = len(self.results)
        successful_scripts = len([r for r in self.results if r['success']])
        failed_scripts = total_scripts - successful_scripts
        
        print(f"Total Scripts: {total_scripts}")
        print(f"✅ Successful: {successful_scripts}")
        print(f"❌ Failed: {failed_scripts}")
        print(f"Success Rate: {(successful_scripts/total_scripts)*100:.1f}%" if total_scripts > 0 else "N/A")
        
        if failed_scripts > 0:
            print(f"\n❌ FAILED SCRIPTS:")
            for result in self.results:
                if not result['success']:
                    print(f"  - {result['script']}: {result['message']}")
        
        if successful_scripts > 0:
            print(f"\n✅ SUCCESSFUL SCRIPTS:")
            for result in self.results:
                if result['success']:
                    print(f"  - {result['script']}: {result['row_count']} rows")
    
    def cleanup(self):
        """Clean up Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """Main execution function"""
    # Get the directory containing the SQL scripts
    current_dir = Path(__file__).parent
    sources_dir = current_dir / "sources"
    
    if not sources_dir.exists():
        print(f"ERROR: Sources directory not found: {sources_dir}")
        sys.exit(1)
    
    validator = SparkSQLValidator()
    
    try:
        # Run validation
        results = validator.validate_all_scripts(sources_dir)
        
        # Print summary
        validator.print_summary()
        
        # Exit with error code if any scripts failed
        failed_count = len([r for r in results if not r['success']])
        if failed_count > 0:
            print(f"\n⚠️  {failed_count} scripts failed validation")
            sys.exit(1)
        else:
            print(f"\n🎉 All scripts validated successfully!")
            sys.exit(0)
            
    except Exception as e:
        print(f"\nERROR: Validation process failed: {e}")
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        validator.cleanup()


if __name__ == "__main__":
    main() 