"""
PySpark pipeline with multiple aggregations (sum, min, max, avg) per group.
Tests migration path for complex aggregate expressions.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, min as spark_min, max as spark_max, avg


def setup_views(spark: SparkSession) -> None:
    """Create temp view sales with amount and region."""
    rows = [
        ("east", 100.0),
        ("east", 150.0),
        ("east", 80.0),
        ("west", 200.0),
        ("west", 120.0),
    ]
    spark.createDataFrame(rows, "region string, amount double").createOrReplaceTempView(
        "sales"
    )


def run(spark: SparkSession):
    """Aggregate sales by region: count, sum, min, max, avg amount."""
    df = spark.table("sales")
    return df.groupBy("region").agg(
        count("*").alias("tx_count"),
        spark_sum("amount").alias("total"),
        spark_min("amount").alias("min_amount"),
        spark_max("amount").alias("max_amount"),
        avg("amount").alias("avg_amount"),
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MultiAggPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
