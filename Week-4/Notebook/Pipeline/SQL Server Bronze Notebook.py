# Databricks notebook source
# DBTITLE 1,Config — catalog, schema, table list
# ---------------------------------------------------------------------------
# Config — update these to match your pipeline destination
# ---------------------------------------------------------------------------
BRONZE_CATALOG = "vendor_catalog"
BRONZE_SCHEMA  = "bronze"

# List the tables ingested by sql_server_ingestion_pipeline (add/remove as needed)
BRONZE_TABLES = [
    "customers",
    "orders",
    "products",
]

print(f"Bronze layer  : {BRONZE_CATALOG}.{BRONZE_SCHEMA}")
print(f"Tables tracked: {BRONZE_TABLES}")

# COMMAND ----------

# DBTITLE 1,Row count validation — all bronze tables
# MAGIC %sql
# MAGIC -- Row count across all bronze tables landed by sql_server_ingestion_pipeline
# MAGIC SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders',   COUNT(*) FROM vendor_catalog.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*) FROM vendor_catalog.bronze.products
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Sample — Bronze customers
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.bronze.customers LIMIT 10

# COMMAND ----------

# DBTITLE 1,Sample — Bronze orders
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.bronze.orders LIMIT 10

# COMMAND ----------

# DBTITLE 1,Sample — Bronze products
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.bronze.products LIMIT 10

# COMMAND ----------

# DBTITLE 1,Null / duplicate check — Bronze orders
# MAGIC %sql
# MAGIC -- Check for nulls in key columns and duplicate order_ids in bronze
# MAGIC SELECT
# MAGIC   COUNT(*)                                        AS total_rows,
# MAGIC   COUNT(CASE WHEN order_id   IS NULL THEN 1 END)  AS null_order_id,
# MAGIC   COUNT(CASE WHEN customer_id IS NULL THEN 1 END) AS null_customer_id,
# MAGIC   COUNT(CASE WHEN product_id  IS NULL THEN 1 END) AS null_product_id,
# MAGIC   COUNT(*) - COUNT(DISTINCT order_id)             AS duplicate_order_ids
# MAGIC FROM vendor_catalog.bronze.orders