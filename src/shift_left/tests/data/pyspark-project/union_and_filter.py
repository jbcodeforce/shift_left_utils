"""
PySpark pipeline that unions two DataFrames then filters and aggregates.
Tests migration path for Union and chained Filter/Aggregate in Catalyst plan.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def setup_views(spark: SparkSession) -> None:
    """Create two temp views to union."""
    stream_a = [
        ("a", "click", 1),
        ("a", "view", 2),
        ("b", "click", 1),
    ]
    stream_b = [
        ("a", "click", 2),
        ("c", "view", 1),
    ]
    spark.createDataFrame(
        stream_a, "user_id string, action string, session_id int"
    ).createOrReplaceTempView("stream_a")
    spark.createDataFrame(
        stream_b, "user_id string, action string, session_id int"
    ).createOrReplaceTempView("stream_b")


def run(spark: SparkSession):
    """Union stream_a and stream_b, filter to clicks only, count by user_id."""
    a = spark.table("stream_a")
    b = spark.table("stream_b")
    combined = a.union(b).filter(col("action") == "click")
    return combined.groupBy("user_id").agg(count("*").alias("click_count"))


if __name__ == "__main__":
    spark = SparkSession.builder.appName("UnionPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
