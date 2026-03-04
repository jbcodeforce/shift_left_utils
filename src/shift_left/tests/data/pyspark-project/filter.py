from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def setup_views(spark: SparkSession) -> None:
    """Create temp view ecommerce_events for the pipeline."""
    rows = [
        ("user_1", "purchase"),
        ("user_1", "view"),
        ("user_2", "purchase"),
    ]
    spark.createDataFrame(rows, "user_id string, event_name string").createOrReplaceTempView(
        "ecommerce_events"
    )


def run(spark: SparkSession):
    """Build the pipeline: ecommerce_events -> filter -> groupBy agg. Caller must create the view."""
    df = spark.table("ecommerce_events")
    filtered_df = df.filter(col("event_name") == "purchase")
    return filtered_df.groupBy("user_id").agg(count("*").alias("purchase_count"))


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MigrationPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
