# Databricks notebook source
# DBTITLE 1,Set landing paths
from pyspark.sql import functions as F

catalog = "vendor_catalog"
landing_root = "/Volumes/vendor_catalog/default/landing"

source_configs = [
    {
        "name": "products",
        "source_path": f"{landing_root}/products",
        "schema_path": f"{landing_root}/_schemas/vendor_products_csv",
        "checkpoint_path": f"{landing_root}/_checkpoints/vendor_products_csv",
        "bronze_table": f"{catalog}.bronze.vendor_products_csv",
    },
    {
        "name": "customers",
        "source_path": f"{landing_root}/customers",
        "schema_path": f"{landing_root}/_schemas/vendor_customers_csv",
        "checkpoint_path": f"{landing_root}/_checkpoints/vendor_customers_csv",
        "bronze_table": f"{catalog}.bronze.vendor_customers_csv",
    },
    {
        "name": "orders",
        "source_path": f"{landing_root}/orders",
        "schema_path": f"{landing_root}/_schemas/vendor_orders_csv",
        "checkpoint_path": f"{landing_root}/_checkpoints/vendor_orders_csv",
        "bronze_table": f"{catalog}.bronze.vendor_orders_csv",
    },
]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
source_configs

# COMMAND ----------

# DBTITLE 1,Load Bronze tables with Auto Loader
streaming_queries = []

for config in source_configs:
    bronze_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", config["schema_path"])
        .load(config["source_path"])
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["checkpoint_path"])
        .trigger(availableNow=True)
        .toTable(config["bronze_table"])
    )
    streaming_queries.append(query)

for query in streaming_queries:
    query.awaitTermination()

for config in source_configs:
    print(f"{config['bronze_table']}: {spark.table(config['bronze_table']).count()} rows")

# COMMAND ----------

# DBTITLE 1,Query Bronze row counts
# MAGIC %sql
# MAGIC SELECT 'vendor_products_csv' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.vendor_products_csv
# MAGIC UNION ALL
# MAGIC SELECT 'vendor_customers_csv' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.vendor_customers_csv
# MAGIC UNION ALL
# MAGIC SELECT 'vendor_orders_csv' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.vendor_orders_csv
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,Query Bronze sample records
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vendor_catalog.bronze.vendor_orders_csv
# MAGIC ORDER BY order_id
# MAGIC LIMIT 5;