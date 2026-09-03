from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="vendor_catalog.silver.vendor_products_silver",
    comment="Cleaned and deduplicated vendor products"
)
def vendor_products():
    products = spark.read.table("vendor_catalog.bronze.vendor_products_bronze")

    standardized_products = (
        products.select(
            F.col("product_id").cast("int").alias("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("category")).alias("category"),
            F.col("price").cast("decimal(10,2)").alias("price"),
            F.col("_rescued_data"),
            F.col("_ingest_timestamp"),
            F.col("_source_file")
        )
        .filter(F.col("product_id").isNotNull())
    )

    dedupe_window = Window.partitionBy(
        "product_id", "product_name", "category", "price"
    ).orderBy(F.col("_ingest_timestamp").desc(), F.col("_source_file").desc())

    return (
        standardized_products.withColumn("_row_rank", F.row_number().over(dedupe_window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )