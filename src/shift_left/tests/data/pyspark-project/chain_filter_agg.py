"""
PySpark pipeline with chained filter -> aggregate -> filter (having-like).
Tests migration path for multiple Filter and Aggregate nodes in sequence.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


def setup_views(spark: SparkSession) -> None:
    """Create temp view transactions with user_id, amount, and category."""
    rows = [
        ("u1", 10.0, "A"),
        ("u1", 20.0, "A"),
        ("u1", 5.0, "B"),
        ("u2", 100.0, "A"),
        ("u2", 50.0, "B"),
    ]
    spark.createDataFrame(
        rows, "user_id string, amount double, category string"
    ).createOrReplaceTempView("transactions")


def run(spark: SparkSession):
    """Filter to category A, aggregate by user_id, then filter to users with count >= 2."""
    df = spark.table("transactions").filter(col("category") == "A")
    agg_df = df.groupBy("user_id").agg(
        count("*").alias("tx_count"),
        spark_sum("amount").alias("total"),
    )
    return agg_df.filter(col("tx_count") >= 2)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ChainFilterAggPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
