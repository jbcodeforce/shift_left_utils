#!/bin/bash

# Spark SQL Script Validation Runner
# This script sets up the environment and runs the Spark SQL validation

set -e

echo "🚀 Spark SQL Script Validation Runner"
echo "======================================"

# Check if we're in the right directory
if [ ! -f "validate_spark_scripts.py" ]; then
    echo "❌ ERROR: validate_spark_scripts.py not found"
    echo "   Please run this script from the spark-project directory"
    exit 1
fi

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
echo "🐍 Using Python: $python_version"

# Check if PySpark is installed
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "📦 PySpark not found. Installing..."
    if [ -f "requirements.txt" ]; then
        pip3 install -r requirements.txt
    else
        pip3 install pyspark>=3.5.0
    fi
    echo "✅ PySpark installed successfully"
else
    echo "✅ PySpark is already installed"
fi

# Check Java (required for Spark)
if ! command -v java &> /dev/null; then
    echo "⚠️  WARNING: Java not found. PySpark requires Java 8 or later."
    echo "   Please install Java and set JAVA_HOME if you encounter issues."
fi

# Set environment variables for better Spark performance
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Run the validation
echo ""
echo "🔍 Starting validation..."
echo "========================"

if python3 validate_spark_scripts.py; then
    echo ""
    echo "🎉 Validation completed successfully!"
    exit 0
else
    echo ""
    echo "❌ Validation failed. Check the output above for details."
    exit 1
fi 