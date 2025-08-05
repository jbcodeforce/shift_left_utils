from pathlib import Path
import sys
from typing import Tuple
import unittest
from datetime import datetime, timedelta, date


try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, 
        StructField, 
        StringType, 
        TimestampType, 
        DateType, 
        DoubleType, 
        ArrayType,
        IntegerType,
        MapType,
        MapStruct
    )
except ImportError:
    print("ERROR: PySpark not installed. Please run: pip install pyspark")
    sys.exit(1)

class TestSparkScripts(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestSparkScripts").getOrCreate()
        cls.current_dir = Path(__file__).parent
        cls.sources_dir = cls.current_dir / "sources"
    
        if not cls.sources_dir.exists():
            print(f"ERROR: Sources directory not found: {cls.sources_dir}")
            sys.exit(1)

    def setUp(self):
        """Setup method called before each test."""
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

    def _read_sql_file(self, file_path: Path) -> str:
        """Read SQL content from file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read().strip()
                sql_lines = [line.strip() for line in sql_content.split('\n') if line.strip() and not line.strip().startswith('--')]
                return '\n'.join(sql_lines)
        except Exception as e:
            raise Exception(f"Failed to read file {file_path}: {e}")
    
    def _create_web_events(self):
        # Create events with timestamps spanning 2 hours from 2 days ago
        from datetime import datetime, timedelta
        # Start from 2 days ago at 10:00 AM
        base_time = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0) - timedelta(days=2)
        
        # Create timestamps with 30-minute intervals over 2 hours
        start_time = base_time
        mid_time = base_time + timedelta(minutes=30)
        end_time = base_time + timedelta(hours=2)
        
        web_events_data = [
            ("user_001", "session_001", start_time, "page_view", "/home"),
            ("user_001", "session_001", mid_time, "page_view", "/products"),
            ("user_001", "session_001", end_time, "purchase", "/checkout"),
            ("user_002", "session_002", start_time, "page_view", "/home"),
            ("user_002", "session_002", end_time, "purchase", "/checkout"), 
            ("user_003", "session_003", start_time, "page_view", "/home"),
            ("user_003", "session_003", mid_time, "page_view", "/products"),
            ("user_003", "session_003", end_time, "purchase", "/checkout"),
        ]
        
        web_events_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("page_url", StringType(), True)
        ])
        
        web_events = self.spark.createDataFrame(web_events_data, web_events_schema)
        web_events.createOrReplaceTempView("web_events")

    def _create_customer_profiles(self):
        base_time = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0) - timedelta(days=2)
        
        customer_profiles_data = [
            ("user_001", "25-34", "US", "premium", base_time - timedelta(days=25)),
            ("user_002", "35-44", "UK", "basic", date(2023, 6, 20)),
            ("user_003", "18-24", "CA", "premium", base_time - timedelta(days=60)),
        ]
        
        customer_profiles_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("age_group", StringType(), True),
            StructField("location", StringType(), True),
            StructField("membership_tier", StringType(), True),
            StructField("registration_date", DateType(), True)
        ])
        
        customer_profiles = self.spark.createDataFrame(customer_profiles_data, customer_profiles_schema)
        customer_profiles.createOrReplaceTempView("customer_profiles")


    def _create_purchases(self):
        base_time = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0) - timedelta(days=2)
        purchases_data = [
            ("user_001", 299.99, base_time - timedelta(days=25)),
            ("user_001", 149.99, base_time - timedelta(days=20)),
            ("user_002", 79.99, base_time - timedelta(days=40)),
            ("user_003", 24.99, base_time - timedelta(days=10)),
        ]
        purchases_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("purchase_date", DateType(), True)
        ])
        
        purchases = self.spark.createDataFrame(purchases_data, purchases_schema)
        purchases.createOrReplaceTempView("purchases")

    def _create_raw_events(self):
        base_time = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0) - timedelta(days=2)
        
        raw_events_data = [
            ("evt_001", "user_001", base_time, "click", 
             {"tags": ["premium", "mobile"], 
             "custom_fields": {"campaign_id": "camp_001", "source": "organic"}},
             {"device_info": {"os": "iOS", "browser": "Safari"}, 
             "location": {"country": "US", "city": "New York"}},
             '{"user": {"preferences": {"language": "en"}},"session": {"duration": "300"}}'),
        ]
        raw_events_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),  
            StructField("event_type", StringType(), True),
            StructField("event_properties", MapStruct(StringType(), ArrayType(StringType())), True),
            StructField("event_metadata", MapType(StringType(), MapType(StringType(), StringType())), True),
            StructField("event_properties_json", StringType(), True)
        ])
        
        raw_events = self.spark.createDataFrame(raw_events_data, raw_events_schema)
        raw_events.createOrReplaceTempView("raw_events")

    def _execute_sql_script(self, script_name: str, sql_content: str) -> Tuple[bool, str, int]:
        try:
            result_df = self.spark.sql(sql_content)
            row_count = result_df.count()
            if row_count >= 0:
                print(f"    ✓ Query executed successfully, returned {row_count} rows")
                # Uncomment the next line to see sample data:
                print(result_df.show(5, truncate=False))
                return True, f"Success: {row_count} rows returned", row_count
            else:
                print(f"    ✗ Query executed successfully, returned 0 row")
                return False, "Query executed successfully, returned 0 row", 0
        except Exception as e:
            error_msg = f"Failed to execute SQL: {str(e)}"
            print(f"    ✗ Query failed: {error_msg}")
            self.fail(error_msg)

    def _test_customer_journey(self):
        print("Running test_customer_journey")
        self._create_web_events()
        self._create_customer_profiles()
        self._create_purchases()
        success, message, row_count = self._read_sql_file(self.sources_dir / "src_customer_journey.sql")
        assert success
        assert row_count > 0
        print(message)

    def _test_advanced_transformations(self):
        sql_content = self._read_sql_file(self.sources_dir / "src_advanced_transformations.sql")
        self.spark.sql(sql_content)
        self.spark.sql("select * from advanced_transformations").show()

    def test_event_processing(self):
        self._create_raw_events()
        sql_content = self._read_sql_file(self.sources_dir / "src_event_processing.sql")
        self.spark.sql(sql_content)
        self.spark.sql("select * from event_processing").show()

if __name__ == "__main__":
    print("🚀 Starting Spark SQL Script Validation")
    unittest.main()