"""
PySpark pipeline joining orders with customers and aggregating per customer.
Used to test automatic migration path (Catalyst plan export and Flink SQL generation).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


def setup_views(spark: SparkSession) -> None:
    """Create temp views for orders and customers."""
    orders_rows = [
        (1, "ord_1", 100.0, "c1"),
        (2, "ord_2", 250.0, "c1"),
        (3, "ord_3", 75.0, "c2"),
    ]
    spark.createDataFrame(
        orders_rows, "order_id int, order_ref string, amount double, customer_id string"
    ).createOrReplaceTempView("orders")

    customers_rows = [
        ("c1", "Alice", "NY"),
        ("c2", "Bob", "CA"),
    ]
    spark.createDataFrame(
        customers_rows, "customer_id string, name string, region string"
    ).createOrReplaceTempView("customers")


def run(spark: SparkSession):
    """Join orders and customers, then aggregate total amount and order count per customer."""
    orders = spark.table("orders")
    customers = spark.table("customers")
    joined = orders.join(
        customers, orders.customer_id == customers.customer_id, "inner"
    ).select(
        col("orders.customer_id"),
        col("customers.name"),
        col("customers.region"),
        col("orders.amount"),
    )
    return joined.groupBy("customer_id", "name", "region").agg(
        spark_sum("amount").alias("total_amount"),
        count("*").alias("order_count"),
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("JoinsPoC").getOrCreate()
    setup_views(spark)
    result_df = run(spark)
    result_df.show()
