"""
PySpark pipeline with complex select expressions: when/case, literal, and aliases.
Tests migration path for Project with conditional and literal expressions.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count


def setup_views(spark: SparkSession) -> None:
    """Create temp view with status and amount."""
    rows = [
        ("pending", 50.0),
        ("completed", 100.0),
        ("pending", 25.0),
        ("cancelled", 200.0),
    ]
    spark.createDataFrame(
        rows, "status string, amount double"
    ).createOrReplaceTempView("orders")


def run(spark: SparkSession):
    """Filter non-cancelled, add flag column via when/otherwise, then aggregate."""
    df = spark.table("orders").filter(col("status") != "cancelled")
    with_flag = df.withColumn(
        "is_completed",
        when(col("status") == "completed", lit(1)).otherwise(lit(0)),
    ).select(
        col("status"),
        col("amount"),
        col("is_completed"),
    )
    return with_flag.groupBy("status", "is_completed").agg(
        count("*").alias("cnt"),
        count("amount").alias("amount_cnt"),
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SelectExprPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
