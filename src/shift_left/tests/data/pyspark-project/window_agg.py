"""
PySpark pipeline using window functions: row_number and sum over partition.
Tests migration path for window expressions in Catalyst plan.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.window import Window


def setup_views(spark: SparkSession) -> None:
    """Create temp view events with user_id, event_ts, and value."""
    rows = [
        ("u1", "2024-01-01 10:00:00", 10),
        ("u1", "2024-01-01 10:05:00", 20),
        ("u1", "2024-01-01 10:10:00", 5),
        ("u2", "2024-01-01 11:00:00", 30),
    ]
    spark.createDataFrame(
        rows, "user_id string, event_ts string, value int"
    ).createOrReplaceTempView("events")


def run(spark: SparkSession):
    """Per-user running sum of value ordered by event_ts using a window."""
    df = spark.table("events")
    window_spec = Window.partitionBy("user_id").orderBy(col("event_ts"))
    return df.withColumn(
        "running_sum", spark_sum("value").over(window_spec)
    ).select("user_id", "event_ts", "value", "running_sum")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("WindowPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
