#!/usr/bin/env python3
"""
Test script for the vLLM-based KSQL to Flink SQL agent.

This script demonstrates how to use the KsqlToFlinkSqlVllmAgent class
to translate KSQL statements to Flink SQL using vLLM with cogito model.

Usage:
    python test_vllm_agent.py

Environment Variables:
    VLLM_MODEL: Model name (default: cogito:32b)
    VLLM_BASE_URL: vLLM server URL (default: http://localhost:8000)
    VLLM_API_KEY: API key for vLLM (default: token)
"""

import os
import sys
from typing import List, Tuple

try:
    from ksql_vllm_code_agent import KsqlToFlinkSqlVllmAgent
except ImportError:
    print("Error: Could not import KsqlToFlinkSqlVllmAgent")
    print("Make sure you're running this from the correct directory")
    sys.exit(1)


def test_simple_stream():
    """Test translation of a simple KSQL stream"""
    print("=== Testing Simple KSQL Stream Translation ===")
    
    ksql_input = """
    CREATE STREAM movements (
        person VARCHAR KEY, 
        location VARCHAR
    ) WITH (
        VALUE_FORMAT='JSON', 
        PARTITIONS=1, 
        KAFKA_TOPIC='movements'
    );
    """
    
    agent = KsqlToFlinkSqlVllmAgent()
    
    try:
        ddl_statements, dml_statements = agent.translate_from_ksql_to_flink_sql(
            ksql_input, 
            validate=False
        )
        
        print("✅ Translation successful!")
        print("\nDDL Statements:")
        for i, ddl in enumerate(ddl_statements):
            print(f"DDL {i+1}:")
            print(ddl)
            print()
            
        print("DML Statements:")
        for i, dml in enumerate(dml_statements):
            print(f"DML {i+1}:")
            print(dml)
            print()
            
    except Exception as e:
        print(f"❌ Translation failed: {e}")


def test_complex_aggregation():
    """Test translation of KSQL with aggregation"""
    print("=== Testing Complex KSQL Aggregation Translation ===")
    
    ksql_input = """
    CREATE TABLE person_stats WITH (VALUE_FORMAT='AVRO') AS
    SELECT 
        person,
        LATEST_BY_OFFSET(location) AS latest_location,
        COUNT(*) AS location_changes
    FROM movements
    GROUP BY person
    EMIT CHANGES;
    """
    
    agent = KsqlToFlinkSqlVllmAgent()
    
    try:
        ddl_statements, dml_statements = agent.translate_from_ksql_to_flink_sql(
            ksql_input, 
            validate=False
        )
        
        print("✅ Translation successful!")
        print("\nDDL Statements:")
        for i, ddl in enumerate(ddl_statements):
            print(f"DDL {i+1}:")
            print(ddl)
            print()
            
        print("DML Statements:")
        for i, dml in enumerate(dml_statements):
            print(f"DML {i+1}:")
            print(dml)
            print()
            
    except Exception as e:
        print(f"❌ Translation failed: {e}")


def test_multiple_statements():
    """Test translation of multiple KSQL statements"""
    print("=== Testing Multiple KSQL Statements Translation ===")
    
    ksql_input = """
    CREATE STREAM raw_events (
        id VARCHAR KEY, 
        data VARCHAR,
        timestamp BIGINT
    ) WITH (
        VALUE_FORMAT='JSON', 
        KAFKA_TOPIC='raw_events'
    );

    CREATE TABLE processed_events WITH (VALUE_FORMAT='AVRO') AS
    SELECT 
        id,
        UCASE(data) as processed_data,
        COUNT(*) as event_count
    FROM raw_events
    GROUP BY id, data
    EMIT CHANGES;
    """
    
    agent = KsqlToFlinkSqlVllmAgent()
    
    try:
        ddl_statements, dml_statements = agent.translate_from_ksql_to_flink_sql(
            ksql_input, 
            validate=False
        )
        
        print("✅ Translation successful!")
        print("\nDDL Statements:")
        for i, ddl in enumerate(ddl_statements):
            print(f"DDL {i+1}:")
            print(ddl)
            print()
            
        print("DML Statements:")
        for i, dml in enumerate(dml_statements):
            print(f"DML {i+1}:")
            print(dml)
            print()
            
    except Exception as e:
        print(f"❌ Translation failed: {e}")


def check_vllm_connection():
    """Check if vLLM server is accessible"""
    print("=== Checking vLLM Connection ===")
    
    vllm_base_url = os.getenv("VLLM_BASE_URL", "http://localhost:8000")
    model_name = os.getenv("VLLM_MODEL", "cogito:32b")
    
    print(f"vLLM Base URL: {vllm_base_url}")
    print(f"Model Name: {model_name}")
    
    try:
        import requests
        response = requests.get(f"{vllm_base_url}/health", timeout=5)
        if response.status_code == 200:
            print("✅ vLLM server is accessible")
            return True
        else:
            print(f"❌ vLLM server returned status: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to vLLM server: {e}")
        print("Please ensure vLLM server is running and accessible")
        return False


def main():
    """Main test function"""
    print("🚀 vLLM KSQL to Flink SQL Agent Test Suite")
    print("=" * 50)
    
    # Check vLLM connection
    if not check_vllm_connection():
        print("\n⚠️  vLLM server not accessible. Tests may fail.")
        print("To start vLLM server with cogito model:")
        print("```")
        print("# Install vLLM")
        print("pip install vllm")
        print("")
        print("# Start vLLM server with cogito model")
        print("python -m vllm.entrypoints.openai.api_server \\")
        print("  --model cogito:32b \\")
        print("  --host 0.0.0.0 \\")
        print("  --port 8000")
        print("```")
        print()
    
    # Run tests
    try:
        test_simple_stream()
        print()
        
        test_complex_aggregation()
        print()
        
        test_multiple_statements()
        print()
        
        print("🎉 All tests completed!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main() 