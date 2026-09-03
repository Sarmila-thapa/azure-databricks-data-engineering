from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="vendor_catalog.silver.vendor_customers_silver",
    comment="Cleaned and deduplicated vendor customers"
)
def vendor_customers():
    customers = spark.read.table("vendor_catalog.bronze.vendor_customers_bronze")

    standardized_customers = (
        customers.select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim(F.col("first_name")).alias("first_name"),
            F.trim(F.col("last_name")).alias("last_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("country")).alias("country"),
            F.trim(F.col("city")).alias("city"),
            F.col("_rescued_data"),
            F.col("_ingest_timestamp"),
            F.col("_source_file")
        )
        .filter(F.col("customer_id").isNotNull())
    )

    dedupe_window = Window.partitionBy("customer_id").orderBy(
        F.col("_ingest_timestamp").desc(),
        F.col("city").desc(),
        F.col("email").desc()
    )

    return (
        standardized_customers.withColumn("_row_rank", F.row_number().over(dedupe_window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )