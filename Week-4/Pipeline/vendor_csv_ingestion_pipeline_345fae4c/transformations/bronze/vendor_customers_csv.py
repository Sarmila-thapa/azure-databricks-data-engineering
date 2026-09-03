from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="vendor_catalog.bronze.vendor_customers_bronze",
    comment="Bronze Auto Loader ingestion for vendor customers CSV files"
)
def vendor_customers_csv():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/vendor_catalog/default/landing/customers")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )