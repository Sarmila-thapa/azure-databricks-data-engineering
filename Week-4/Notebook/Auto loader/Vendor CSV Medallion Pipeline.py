# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Set catalog and landing paths
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

for schema_name in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

source_configs

# COMMAND ----------

# DBTITLE 1,Load landing CSV files into Bronze with Auto Loader
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
    row_count = spark.table(config["bronze_table"]).count()
    print(f"{config['bronze_table']}: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Build Silver Delta tables
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS vendor_catalog.silver.vendor_products AS
# MAGIC SELECT product_id,
# MAGIC        product_name,
# MAGIC        category,
# MAGIC        price,
# MAGIC        _ingest_timestamp,
# MAGIC        _source_file
# MAGIC FROM (
# MAGIC   SELECT CAST(product_id AS INT) AS product_id,
# MAGIC          TRIM(product_name) AS product_name,
# MAGIC          TRIM(category) AS category,
# MAGIC          CAST(price AS DECIMAL(10,2)) AS price,
# MAGIC          _ingest_timestamp,
# MAGIC          _source_file,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY CAST(product_id AS INT), TRIM(product_name), TRIM(category), CAST(price AS DECIMAL(10,2))
# MAGIC            ORDER BY _ingest_timestamp DESC, _source_file DESC
# MAGIC          ) AS rn
# MAGIC   FROM vendor_catalog.bronze.vendor_products_csv
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vendor_catalog.silver.vendor_customers AS
# MAGIC SELECT customer_id,
# MAGIC        first_name,
# MAGIC        last_name,
# MAGIC        email,
# MAGIC        country,
# MAGIC        city,
# MAGIC        _ingest_timestamp,
# MAGIC        _source_file
# MAGIC FROM (
# MAGIC   SELECT CAST(customer_id AS INT) AS customer_id,
# MAGIC          TRIM(first_name) AS first_name,
# MAGIC          TRIM(last_name) AS last_name,
# MAGIC          LOWER(TRIM(email)) AS email,
# MAGIC          TRIM(country) AS country,
# MAGIC          TRIM(city) AS city,
# MAGIC          _ingest_timestamp,
# MAGIC          _source_file,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY CAST(customer_id AS INT)
# MAGIC            ORDER BY _ingest_timestamp DESC, TRIM(city) DESC, LOWER(TRIM(email)) DESC
# MAGIC          ) AS rn
# MAGIC   FROM vendor_catalog.bronze.vendor_customers_csv
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vendor_catalog.silver.vendor_orders AS
# MAGIC SELECT order_id,
# MAGIC        customer_id,
# MAGIC        product_id,
# MAGIC        quantity,
# MAGIC        order_date,
# MAGIC        _ingest_timestamp,
# MAGIC        _source_file
# MAGIC FROM (
# MAGIC   SELECT CAST(order_id AS INT) AS order_id,
# MAGIC          CAST(customer_id AS INT) AS customer_id,
# MAGIC          CAST(product_id AS INT) AS product_id,
# MAGIC          CAST(quantity AS INT) AS quantity,
# MAGIC          TO_DATE(order_date) AS order_date,
# MAGIC          _ingest_timestamp,
# MAGIC          _source_file,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY CAST(order_id AS INT), CAST(customer_id AS INT), CAST(product_id AS INT), CAST(quantity AS INT), TO_DATE(order_date)
# MAGIC            ORDER BY _ingest_timestamp DESC, _source_file DESC
# MAGIC          ) AS rn
# MAGIC   FROM vendor_catalog.bronze.vendor_orders_csv
# MAGIC )
# MAGIC WHERE rn = 1;

# COMMAND ----------

# DBTITLE 1,Build Gold Delta table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS vendor_catalog.gold.vendor_order_sales AS
# MAGIC SELECT o.order_date,
# MAGIC        o.order_id,
# MAGIC        o.customer_id,
# MAGIC        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
# MAGIC        c.country,
# MAGIC        c.city,
# MAGIC        o.product_id,
# MAGIC        p.product_name,
# MAGIC        p.category,
# MAGIC        o.quantity,
# MAGIC        p.price,
# MAGIC        CAST(o.quantity * p.price AS DECIMAL(12,2)) AS sales_amount
# MAGIC FROM vendor_catalog.silver.vendor_orders o
# MAGIC LEFT JOIN vendor_catalog.silver.vendor_customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC LEFT JOIN vendor_catalog.silver.vendor_products p
# MAGIC   ON o.product_id = p.product_id;

# COMMAND ----------

# DBTITLE 1,Validate row counts
# MAGIC %sql
# MAGIC SELECT 'bronze.vendor_products_csv' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.vendor_products_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.vendor_customers_csv' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.vendor_customers_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.vendor_orders_csv' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.vendor_orders_csv
# MAGIC UNION ALL
# MAGIC SELECT 'silver.vendor_products' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.vendor_products
# MAGIC UNION ALL
# MAGIC SELECT 'silver.vendor_customers' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.vendor_customers
# MAGIC UNION ALL
# MAGIC SELECT 'silver.vendor_orders' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.vendor_orders
# MAGIC UNION ALL
# MAGIC SELECT 'gold.vendor_order_sales' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.gold.vendor_order_sales
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,Inspect Gold sample data
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vendor_catalog.gold.vendor_order_sales
# MAGIC ORDER BY order_date, order_id
# MAGIC LIMIT 5;