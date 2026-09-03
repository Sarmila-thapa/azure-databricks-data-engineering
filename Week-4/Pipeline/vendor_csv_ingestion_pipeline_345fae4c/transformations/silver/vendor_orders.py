from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="vendor_catalog.silver.vendor_orders_silver",
    comment="Cleaned and deduplicated vendor orders"
)
def vendor_orders():
    orders = spark.read.table("vendor_catalog.bronze.vendor_orders_bronze")

    standardized_orders = (
        orders.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.to_date(F.col("order_date")).alias("order_date"),
            F.col("_rescued_data"),
            F.col("_ingest_timestamp"),
            F.col("_source_file")
        )
        .filter(F.col("order_id").isNotNull())
    )

    dedupe_window = Window.partitionBy(
        "order_id", "customer_id", "product_id", "quantity", "order_date"
    ).orderBy(F.col("_ingest_timestamp").desc(), F.col("_source_file").desc())

    return (
        standardized_orders.withColumn("_row_rank", F.row_number().over(dedupe_window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )